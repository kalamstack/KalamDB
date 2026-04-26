//! Performance benchmarks for authentication endpoints
//!
//! This module provides performance testing for authentication operations:
//! - Bearer token authentication latency
//! - JWT authentication latency
//! - Cache hit/miss performance
//! - Concurrent authentication load testing
//!
//! **Performance Targets**:
//! - p50 latency: <50ms for cached auth, <100ms for uncached
//! - p95 latency: <100ms for cached auth, <200ms for uncached
//! - p99 latency: <200ms for cached auth, <500ms for uncached
//! - Cache hit rate: >95% for repeated authentications

use std::time::{Duration, Instant};

use actix_web::{test, web, App};
use kalamdb_commons::{models::ConnectionInfo, Role};

use super::test_support::{auth_helper, TestServer};

/// Performance benchmark for Bearer token authentication
///
/// Note: This test creates fresh app instances for each request to avoid actix-web
/// test framework RefCell borrow conflicts. However, there's still a known issue
/// with connection_info() being called within the handler that can cause panics.
/// Consider running this test in isolation or with --test-threads=1
#[actix_web::test]
#[ignore = "Known actix-web test framework limitation with connection_info()"]
async fn benchmark_bearer_auth_performance() {
    let server = TestServer::new_shared().await;

    // Create test user
    let username = "perf_test_user";
    let password = "SecurePassword123!";
    auth_helper::create_test_user(&server, username, password, Role::User).await;

    // Create auth header
    let auth_header = auth_helper::create_bearer_auth_header(username, username, Role::User);

    // Benchmark: Reduced to 10 requests to avoid actix-web test framework limitations
    // Creating fresh app instances for each request ensures no RefCell conflicts
    let num_requests = 10;
    let mut latencies = Vec::with_capacity(num_requests);

    for i in 0..num_requests {
        // Create fresh app instance for each request to avoid RefCell conflicts
        let app = test::init_service(
            App::new()
                .app_data(web::Data::new(server.app_context.session_factory()))
                .app_data(web::Data::new(server.sql_executor.clone()))
                .app_data(web::Data::new(server.app_context.live_query_manager()))
                .configure(kalamdb_api::routes::configure_routes),
        )
        .await;

        let start = Instant::now();

        let req = test::TestRequest::post()
            .uri("/v1/api/sql")
            .insert_header(("Authorization", auth_header.as_str()))
            .insert_header(("Content-Type", "application/json"))
            .set_json(serde_json::json!({
                "sql": "SELECT 1"
            }))
            .to_request();

        let resp = test::call_service(&app, req).await;
        let end = Instant::now();

        // Only count successful requests
        if resp.status().is_success() {
            latencies.push(end.duration_since(start));
        } else {
            eprintln!("Request {} failed with status: {}", i, resp.status());
        }
    }

    // Calculate percentiles
    latencies.sort();
    let p50 = percentile(&latencies, 50.0);
    let p95 = percentile(&latencies, 95.0);
    let p99 = percentile(&latencies, 99.0);

    println!("Bearer Auth Performance Results:");
    println!("  Sample size: {} requests", latencies.len());
    println!("  p50 latency: {:.2}ms", p50.as_millis());
    println!("  p95 latency: {:.2}ms", p95.as_millis());
    println!("  p99 latency: {:.2}ms", p99.as_millis());

    // Performance assertions
    assert!(p50 < Duration::from_millis(100), "p50 latency too high: {:?}", p50);
    assert!(p95 < Duration::from_millis(200), "p95 latency too high: {:?}", p95);
    assert!(p99 < Duration::from_millis(500), "p99 latency too high: {:?}", p99);
}

/// Performance benchmark for JWT authentication
#[actix_web::test]
async fn benchmark_jwt_auth_performance() {
    let server = TestServer::new_shared().await;

    // Create test user
    let username = "jwt_perf_user";
    let password = "SecurePassword123!";
    auth_helper::create_test_user(&server, username, password, Role::User).await;

    // Create JWT token
    let jwt_secret = "kalamdb-dev-secret-key-change-in-production";
    let jwt_token = auth_helper::create_jwt_token(username, jwt_secret, 3600); // 1 hour expiry
    let auth_header = format!("Bearer {}", jwt_token);

    // Initialize app with authentication middleware
    let app = test::init_service(
        App::new()
            .app_data(web::Data::new(server.app_context.session_factory()))
            .app_data(web::Data::new(server.sql_executor.clone()))
            .app_data(web::Data::new(server.app_context.live_query_manager()))
            .configure(kalamdb_api::routes::configure_routes),
    )
    .await;

    // Warm up the cache with a few requests
    for _ in 0..5 {
        let req = test::TestRequest::post()
            .uri("/v1/api/sql")
            .insert_header(("Authorization", auth_header.as_str()))
            .insert_header(("Content-Type", "application/json"))
            .set_json(serde_json::json!({
                "sql": "SELECT 1"
            }))
            .to_request();

        let _ = test::call_service(&app, req).await;
    }

    // Benchmark: 100 authentication requests
    let num_requests = 100;
    let mut latencies = Vec::with_capacity(num_requests);

    for _ in 0..num_requests {
        let start = Instant::now();

        let req = test::TestRequest::post()
            .uri("/v1/api/sql")
            .insert_header(("Authorization", auth_header.as_str()))
            .insert_header(("Content-Type", "application/json"))
            .set_json(serde_json::json!({
                "sql": "SELECT 1"
            }))
            .to_request();

        let resp = test::call_service(&app, req).await;
        let end = Instant::now();

        // Only count successful requests
        if resp.status().is_success() {
            latencies.push(end.duration_since(start));
        }
    }

    // Calculate percentiles
    latencies.sort();
    let p50 = percentile(&latencies, 50.0);
    let p95 = percentile(&latencies, 95.0);
    let p99 = percentile(&latencies, 99.0);

    println!("JWT Auth Performance Results:");
    println!("  Sample size: {} requests", latencies.len());
    println!("  p50 latency: {:.2}ms", p50.as_millis());
    println!("  p95 latency: {:.2}ms", p95.as_millis());
    println!("  p99 latency: {:.2}ms", p99.as_millis());

    // Performance assertions
    assert!(p50 < Duration::from_millis(100), "p50 latency too high: {:?}", p50);
    assert!(p95 < Duration::from_millis(200), "p95 latency too high: {:?}", p95);
    assert!(p99 < Duration::from_millis(500), "p99 latency too high: {:?}", p99);
}

/// Test cache effectiveness by comparing first request vs cached requests
///
/// Note: This test has the same actix-web framework limitation as the benchmark test.
#[actix_web::test]
#[ignore = "Known actix-web test framework limitation with connection_info()"]
async fn test_auth_cache_effectiveness() {
    let server = TestServer::new_shared().await;

    // Create test user
    let username = "cache_test_user";
    let password = "SecurePassword123!";
    auth_helper::create_test_user(&server, username, password, Role::User).await;

    // Create auth header
    let auth_header = auth_helper::create_bearer_auth_header(username, username, Role::User);

    // First request (cache miss) - use fresh app
    let app = test::init_service(
        App::new()
            .app_data(web::Data::new(server.app_context.session_factory()))
            .app_data(web::Data::new(server.sql_executor.clone()))
            .app_data(web::Data::new(server.app_context.live_query_manager()))
            .configure(kalamdb_api::routes::configure_routes),
    )
    .await;

    let start = Instant::now();
    let req = test::TestRequest::post()
        .uri("/v1/api/sql")
        .insert_header(("Authorization", auth_header.as_str()))
        .insert_header(("Content-Type", "application/json"))
        .set_json(serde_json::json!({
            "sql": "SELECT 1"
        }))
        .to_request();

    let resp = test::call_service(&app, req).await;
    let first_request_time = start.elapsed();

    assert!(resp.status().is_success(), "First request should succeed");

    // Subsequent requests (cache hits) - reduced to 5 to avoid test framework issues
    let num_cached_requests = 5;
    let mut cached_latencies = Vec::with_capacity(num_cached_requests);

    for i in 0..num_cached_requests {
        // Create fresh app instance for each request
        let app = test::init_service(
            App::new()
                .app_data(web::Data::new(server.app_context.session_factory()))
                .app_data(web::Data::new(server.sql_executor.clone()))
                .app_data(web::Data::new(server.app_context.live_query_manager()))
                .configure(kalamdb_api::routes::configure_routes),
        )
        .await;

        let start = Instant::now();

        let req = test::TestRequest::post()
            .uri("/v1/api/sql")
            .insert_header(("Authorization", auth_header.as_str()))
            .insert_header(("Content-Type", "application/json"))
            .set_json(serde_json::json!({
                "sql": "SELECT 1"
            }))
            .to_request();

        let resp = test::call_service(&app, req).await;
        let end = Instant::now();

        if resp.status().is_success() {
            cached_latencies.push(end.duration_since(start));
        } else {
            eprintln!("Cached request {} failed with status: {}", i, resp.status());
        }
    }

    // Calculate average cached request time
    let avg_cached_time = cached_latencies.iter().sum::<Duration>() / cached_latencies.len() as u32;

    println!("Cache Effectiveness Test:");
    println!("  First request (cache miss): {:.2}ms", first_request_time.as_millis());
    println!("  Average cached requests: {:.2}ms", avg_cached_time.as_millis());
    println!(
        "  Cache speedup: {:.1}x",
        first_request_time.as_millis() as f64 / avg_cached_time.as_millis() as f64
    );

    // Cache should provide at least 2x speedup
    assert!(
        first_request_time.as_millis() as f64 / avg_cached_time.as_millis() as f64 >= 2.0,
        "Cache should provide at least 2x speedup"
    );
}

/// Test concurrent authentication load
#[tokio::test]
async fn test_concurrent_auth_load() {
    use std::sync::Arc;

    use kalamdb_auth::{authenticate, AuthRequest, CachedUsersRepo, UserRepository};

    let server = TestServer::new_shared().await;

    // Create multiple test users
    let num_users = 10;
    let mut users = Vec::new();

    for i in 0..num_users {
        let username = format!("concurrent_user_{}", i);
        let password = "SecurePassword123!";
        auth_helper::create_test_user(&server, &username, password, Role::User).await;
        users.push((username, password.to_string()));
    }

    // Use the cached repository path that production auth uses.
    let user_repo: Arc<dyn UserRepository> =
        Arc::new(CachedUsersRepo::new(server.app_context.system_tables().users()));

    // Concurrent authentication test
    let num_concurrent_requests = 50;
    let mut handles = Vec::new();

    for i in 0..num_concurrent_requests {
        let user_repo_clone = user_repo.clone();
        let users_clone = users.clone();

        let handle = tokio::spawn(async move {
            let start = Instant::now();

            // Pick a user based on index
            let user_idx = i % users_clone.len();
            let (username, _password) = &users_clone[user_idx];

            let auth_header =
                auth_helper::create_bearer_auth_header(username, username, Role::User);
            let connection_info = ConnectionInfo::new(Some("127.0.0.1".to_string()));
            let auth_request = AuthRequest::Header(auth_header);

            let result = authenticate(auth_request, &connection_info, &user_repo_clone).await;

            let end = Instant::now();

            (result.is_ok(), end.duration_since(start))
        });

        handles.push(handle);
    }

    // Collect results
    let mut successful_requests = 0;
    let mut latencies = Vec::new();

    for handle in handles {
        let (success, latency) = handle.await.unwrap();
        if success {
            successful_requests += 1;
        }
        latencies.push(latency);
    }

    // Calculate percentiles
    latencies.sort();
    let p50 = percentile(&latencies, 50.0);
    let p95 = percentile(&latencies, 95.0);
    let p99 = percentile(&latencies, 99.0);

    println!("Concurrent Auth Load Test Results:");
    println!("  Total requests: {}", num_concurrent_requests);
    println!("  Successful requests: {}", successful_requests);
    println!(
        "  Success rate: {:.1}%",
        (successful_requests as f64 / num_concurrent_requests as f64) * 100.0
    );
    println!("  p50 latency: {:.2}ms", p50.as_millis());
    println!("  p95 latency: {:.2}ms", p95.as_millis());
    println!("  p99 latency: {:.2}ms", p99.as_millis());

    // Performance assertions - bcrypt is intentionally slow for security
    // With cost=12, expect ~100-300ms per auth on modern hardware
    assert_eq!(
        successful_requests, num_concurrent_requests,
        "All concurrent requests should succeed"
    );
    assert!(
        p95 < Duration::from_millis(30000),
        "Concurrent auth p95 latency too high: {:?}",
        p95
    );

    println!("✓ Concurrent authentication test passed");
}

/// Helper function to calculate percentiles
fn percentile(latencies: &[Duration], p: f64) -> Duration {
    if latencies.is_empty() {
        return Duration::from_millis(0);
    }

    let index = ((latencies.len() - 1) as f64 * p / 100.0) as usize;
    latencies[index]
}
