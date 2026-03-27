//! Test that parameter validation uses config values from AppContext
//!
//! Verifies that the config centralization allows parameter limits to be
//! read from ServerConfig via AppContext.

#![allow(unused_imports)]

use datafusion::common::ScalarValue;
use kalamdb_core::sql::executor::parameter_validation::{validate_parameters, ParameterLimits};

// #[ignore = "Uses test_app_context which starts Raft - slow test"]
// #[ntest::timeout(30_000)]
// #[tokio::test]
// async fn test_parameter_limits_from_config() {
//     // Initialize AppContext (from test_helpers)
//     let app_context = test_helpers::test_app_context();
//     let config = app_context.config();

//     // Verify execution settings
//     assert_eq!(config.execution.max_parameters, 50);
//     assert_eq!(config.execution.max_parameter_size_bytes, 512 * 1024);

//     // Create ParameterLimits from config
//     let limits = ParameterLimits::from_config(&config.execution);
//     assert_eq!(limits.max_count, 50);
//     assert_eq!(limits.max_size_bytes, 512 * 1024);

//     // Test validation with limits from config
//     let params = vec![
//         ScalarValue::Int32(Some(42)),
//         ScalarValue::Utf8(Some("test".to_string())),
//     ];
//     assert!(validate_parameters(&params, &limits).is_ok());

//     // Test exceeding count limit
//     let too_many_params: Vec<ScalarValue> = (0..51).map(|i| ScalarValue::Int32(Some(i))).collect();
//     let result = validate_parameters(&too_many_params, &limits);
//     assert!(result.is_err());
//     assert!(result.unwrap_err().to_string().contains("Parameter count exceeds limit"));

//     // Test exceeding size limit
//     let large_string = "a".repeat(600_000); // 600KB > 512KB limit
//     let large_params = vec![ScalarValue::Utf8(Some(large_string))];
//     let result = validate_parameters(&large_params, &limits);
//     assert!(result.is_err());
//     assert!(result.unwrap_err().to_string().contains("size exceeds limit"));
// }

// #[ignore = "Uses test_app_context which starts Raft - slow test"]
// #[ntest::timeout(30_000)]
// #[tokio::test]
// async fn test_config_accessible_from_app_context() {
//     // Initialize AppContext
//     let app_context = test_helpers::test_app_context();
//     let config = app_context.config();

//     // Verify basic config fields
//     assert_eq!(app_context.node_id().as_u64(), 1);
//     assert!(!config.storage.data_path.is_empty());

//     // Verify execution settings are present
//     assert!(config.execution.max_parameters > 0);
//     assert!(config.execution.max_parameter_size_bytes > 0);
//     assert!(config.execution.handler_timeout_seconds > 0);
// }
