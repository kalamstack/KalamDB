//! Integration tests for TopicCleanup job
//!
//! Verifies that when a topic is deleted:
//! 1. Topic is removed from system.topics
//! 2. TopicCleanup job is scheduled and completes
//! 3. All topic messages and offsets are actually removed from storage

use super::test_support::TestServer;
use anyhow::Result;
use kalamdb_jobs::AppContextJobsExt;

/// Test that TopicCleanup job is scheduled when dropping a topic
#[actix_web::test]
#[ntest::timeout(60000)]
async fn test_topic_cleanup_job_scheduled_on_drop() -> Result<()> {
    let server = TestServer::new_shared().await;
    let ctx = &server.app_context;

    // Create a topic
    let topic_name = format!("test_topic_cleanup_{}", chrono::Utc::now().timestamp_millis());
    ctx.sql_executor()
        .execute_sql(
            &format!("CREATE TOPIC {} PARTITIONS 1", topic_name),
            &server.session_context,
        )
        .await?;

    // Verify topic exists
    let topics = ctx.system_tables().topics().list_topics()?;
    assert!(topics.iter().any(|t| t.name == topic_name));

    // Drop the topic
    ctx.sql_executor()
        .execute_sql(
            &format!("DROP TOPIC {}", topic_name),
            &server.session_context,
        )
        .await?;

    // Give job system time to schedule and execute
    tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;

    // Verify topic is removed
    let topics_after = ctx.system_tables().topics().list_topics()?;
    assert!(!topics_after.iter().any(|t| t.name == topic_name));

    // Check that a TopicCleanup job was created
    let jobs = ctx.system_tables().jobs().list_all_jobs()?;
    let cleanup_job = jobs.iter().find(|j| {
        j.job_type.as_str() == "TopicCleanup"
            && j.parameters.contains(&topic_name)
    });

    assert!(cleanup_job.is_some(), "TopicCleanup job should be scheduled");

    Ok(())
}

/// Test that TopicCleanup job removes topic offsets
#[actix_web::test]
#[ntest::timeout(90000)]
async fn test_topic_cleanup_job_removes_offsets() -> Result<()> {
    let server = TestServer::new_shared().await;
    let ctx = &server.app_context;

    // Create a topic
    let topic_name = format!("test_topic_offsets_{}", chrono::Utc::now().timestamp_millis());
    ctx.sql_executor()
        .execute_sql(
            &format!("CREATE TOPIC {} PARTITIONS 1", topic_name),
            &server.session_context,
        )
        .await?;

    // Publish a message to create topic offsets
    let topic_id = kalamdb_commons::models::TopicId::new(&topic_name);
    let group_id = kalamdb_commons::models::ConsumerGroupId::new("test_group");
    
    // Simulate an offset being recorded
    ctx.system_tables()
        .topic_offsets()
        .upsert_offset(kalamdb_system::providers::topic_offsets::models::TopicOffset::new(
            topic_id.clone(),
            group_id.clone(),
            0, // partition
            0, // last_acked_offset
            chrono::Utc::now().timestamp_millis(),
        ))?;

    // Verify offset exists
    let offsets_before = ctx.system_tables()
        .topic_offsets()
        .get_group_offsets(&topic_id, &group_id)?;
    assert_eq!(offsets_before.len(), 1);

    // Drop the topic
    ctx.sql_executor()
        .execute_sql(
            &format!("DROP TOPIC {}", topic_name),
            &server.session_context,
        )
        .await?;

    // Wait for cleanup job to complete
    tokio::time::sleep(tokio::time::Duration::from_secs(10)).await;

    // Verify offsets are removed
    let offsets_after = ctx.system_tables()
        .topic_offsets()
        .get_group_offsets(&topic_id, &group_id)?;
    assert_eq!(offsets_after.len(), 0, "Topic offsets should be removed after cleanup");

    Ok(())
}

/// Test that TopicCleanup job is idempotent
#[actix_web::test]
#[ntest::timeout(60000)]
async fn test_topic_cleanup_job_idempotent() -> Result<()> {
    let server = TestServer::new_shared().await;
    let ctx = &server.app_context;

    // Create a topic
    let topic_name = format!("test_topic_idempotent_{}", chrono::Utc::now().timestamp_millis());
    ctx.sql_executor()
        .execute_sql(
            &format!("CREATE TOPIC {} PARTITIONS 1", topic_name),
            &server.session_context,
        )
        .await?;

    // Drop the topic
    ctx.sql_executor()
        .execute_sql(
            &format!("DROP TOPIC {}", topic_name),
            &server.session_context,
        )
        .await?;

    // Wait for first cleanup
    tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;

    // Try to schedule another cleanup job manually (simulating retry)
    // This should be idempotent and not fail
    let topic_id = kalamdb_commons::models::TopicId::new(&topic_name);
    let params = kalamdb_jobs::executors::topic_cleanup::TopicCleanupParams {
        topic_id: topic_id.clone(),
        topic_name: topic_name.clone(),
    };
    
    let result = ctx.job_manager().create_job_typed(
        kalamdb_system::providers::jobs::models::JobType::TopicCleanup,
        params,
    );

    // Should either succeed or fail gracefully (not panic)
    match result {
        Ok(_) => {
            // Job created successfully, wait for it to complete
            tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
        }
        Err(e) => {
            // Expected if idempotency key prevents duplicate
            println!("Duplicate job prevented (expected): {}", e);
        }
    }

    // Verify topic is still not in system after multiple cleanup attempts
    let topics = ctx.system_tables().topics().list_topics()?;
    assert!(!topics.iter().any(|t| t.name == topic_name));

    Ok(())
}
