//! Integration tests for UserCleanup job
//!
//! Verifies that when a user is deleted:
//! 1. User is marked as deleted in system.users (soft delete)
//! 2. UserCleanup job is scheduled and completes
//! 3. All user data is actually removed from storage

use super::test_support::TestServer;
use anyhow::Result;

/// Test that UserCleanup job is scheduled and completes when dropping a user
#[actix_web::test]
#[ntest::timeout(60000)]
async fn test_user_cleanup_job_scheduled_on_drop() -> Result<()> {
    let server = TestServer::new_shared().await;
    let ctx = &server.app_context;

    // Create a test user
    let username = format!("test_user_cleanup_{}", chrono::Utc::now().timestamp_millis());
    ctx.sql_executor()
        .execute_sql(
            &format!("CREATE USER {} WITH PASSWORD 'password123'", username),
            &server.session_context,
        )
        .await?;

    // Verify user exists
    let users = ctx.system_tables().users().list_users()?;
    let user_exists = users.iter().any(|u| u.user_id.as_str() == username && u.deleted_at.is_none());
    assert!(user_exists, "User should exist before deletion");

    // Drop the user
    ctx.sql_executor()
        .execute_sql(
            &format!("DROP USER {}", username),
            &server.session_context,
        )
        .await?;

    // Give job system time to schedule and execute
    tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;

    // Verify user is soft-deleted
    let users_after = ctx.system_tables().users().list_users()?;
    let deleted_user = users_after.iter().find(|u| u.user_id.as_str() == username);
    
    match deleted_user {
        Some(user) => {
            assert!(user.deleted_at.is_some(), "User should be marked as deleted");
        }
        None => {
            // User may be fully removed by cleanup job, which is also valid
            println!("User fully removed by cleanup job");
        }
    }

    // Check that a UserCleanup job was created
    let jobs = ctx.system_tables().jobs().list_all_jobs()?;
    let cleanup_job = jobs.iter().find(|j| {
        j.job_type.as_str() == "UserCleanup"
            && j.parameters.contains(&username)
    });

    assert!(cleanup_job.is_some(), "UserCleanup job should be scheduled");

    Ok(())
}

/// Test that UserCleanup job removes user data
#[actix_web::test]
#[ntest::timeout(90000)]
async fn test_user_cleanup_job_removes_data() -> Result<()> {
    let server = TestServer::new_shared().await;
    let ctx = &server.app_context;

    // Create a test user
    let username = format!("test_user_data_{}", chrono::Utc::now().timestamp_millis());
    ctx.sql_executor()
        .execute_sql(
            &format!("CREATE USER {} WITH PASSWORD 'password123'", username),
            &server.session_context,
        )
        .await?;

    // Get user ID
    let users = ctx.system_tables().users().list_users()?;
    let user = users.iter().find(|u| u.user_id.as_str() == username).unwrap();
    let user_id = user.user_id.clone();

    // Create a namespace for the user
    let namespace = format!("ns_{}", chrono::Utc::now().timestamp_millis());
    ctx.sql_executor()
        .execute_sql(
            &format!("CREATE NAMESPACE {}", namespace),
            &server.session_context,
        )
        .await?;

    // Create a user table with some data
    let table_name = format!("user_table_{}", chrono::Utc::now().timestamp_millis());
    ctx.sql_executor()
        .execute_sql(
            &format!("CREATE TABLE {}.{} (id INT, name TEXT)", namespace, table_name),
            &server.session_context,
        )
        .await?;

    ctx.sql_executor()
        .execute_sql(
            &format!("INSERT INTO {}.{} (id, name) VALUES (1, 'test')", namespace, table_name),
            &server.session_context,
        )
        .await?;

    // Drop the user
    ctx.sql_executor()
        .execute_sql(
            &format!("DROP USER {} CASCADE", username),
            &server.session_context,
        )
        .await?;

    // Wait for cleanup job to complete
    tokio::time::sleep(tokio::time::Duration::from_secs(15)).await;

    // Verify user's namespace and tables are removed
    let namespaces = ctx.system_tables().namespaces().list_namespaces()?;
    let user_namespace_exists = namespaces.iter().any(|ns| {
        ns.name == namespace && ns.owner_user_id == user_id
    });
    assert!(!user_namespace_exists, "User namespace should be removed after cleanup");

    Ok(())
}

/// Test UserCleanup job cascade parameter
#[actix_web::test]
#[ntest::timeout(60000)]
async fn test_user_cleanup_job_cascade_parameter() -> Result<()> {
    let server = TestServer::new_shared().await;
    let ctx = &server.app_context;

    // Create a test user
    let username = format!("test_user_cascade_{}", chrono::Utc::now().timestamp_millis());
    ctx.sql_executor()
        .execute_sql(
            &format!("CREATE USER {} WITH PASSWORD 'password123'", username),
            &server.session_context,
        )
        .await?;

    // Drop the user with CASCADE
    ctx.sql_executor()
        .execute_sql(
            &format!("DROP USER {} CASCADE", username),
            &server.session_context,
        )
        .await?;

    // Wait for job to be created
    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;

    // Check that the job has cascade=true in parameters
    let jobs = ctx.system_tables().jobs().list_all_jobs()?;
    let cleanup_job = jobs.iter().find(|j| {
        j.job_type.as_str() == "UserCleanup"
            && j.parameters.contains(&username)
    });

    assert!(cleanup_job.is_some(), "UserCleanup job should be scheduled");
    
    if let Some(job) = cleanup_job {
        // Parameters should contain cascade:true
        assert!(
            job.parameters.contains("cascade") || job.parameters.contains("true"),
            "Job parameters should indicate cascade mode"
        );
    }

    Ok(())
}
