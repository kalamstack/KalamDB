use super::common::{create_shared_kalam_table_in_schema, drop_kalam_tables, unique_name, TestEnv};

#[tokio::test]
#[ntest::timeout(45000)]
async fn e2e_scenario_iot_agent_fleet_incident_flow() {
    let env = TestEnv::global().await;
    let schema = unique_name("iot_app");
    let devices = unique_name("devices");
    let telemetry = unique_name("telemetry");
    let commands = unique_name("commands");
    let alerts = unique_name("alerts");

    let pg_admin = env.pg_connect().await;
    let ingest_a = env.pg_connect().await;
    let ingest_b = env.pg_connect().await;
    let agent_pg = env.pg_connect().await;
    let operator_pg = env.pg_connect().await;

    create_shared_kalam_table_in_schema(
        &pg_admin,
        &schema,
        &devices,
        "id TEXT, site_id TEXT, firmware TEXT, status TEXT",
    )
    .await;
    create_shared_kalam_table_in_schema(
        &pg_admin,
        &schema,
        &telemetry,
        "id TEXT, device_id TEXT, temperature INTEGER, battery_level INTEGER, health TEXT",
    )
    .await;
    create_shared_kalam_table_in_schema(
        &pg_admin,
        &schema,
        &commands,
        "id TEXT, device_id TEXT, action TEXT, status TEXT, issued_by TEXT",
    )
    .await;
    create_shared_kalam_table_in_schema(
        &pg_admin,
        &schema,
        &alerts,
        "id TEXT, device_id TEXT, severity TEXT, summary TEXT, created_by TEXT",
    )
    .await;

    pg_admin
        .batch_execute(&format!(
            "INSERT INTO {schema}.{devices} (id, site_id, firmware, status) VALUES
             ('dev-1', 'plant-a', '1.0.7', 'online'),
             ('dev-2', 'plant-a', '1.0.7', 'online'),
             ('dev-3', 'plant-b', '1.0.9', 'online');"
        ))
        .await
        .expect("seed device inventory");

    let schema_a = schema.clone();
    let telemetry_a = telemetry.clone();
    let handle_a = tokio::spawn(async move {
        ingest_a
            .batch_execute(&format!(
                "INSERT INTO {schema_a}.{telemetry_a} (id, device_id, temperature, battery_level, \
                 health) VALUES
                 ('t-1', 'dev-1', 74, 58, 'healthy'),
                 ('t-2', 'dev-1', 88, 51, 'degraded'),
                 ('t-3', 'dev-2', 82, 17, 'critical');"
            ))
            .await
            .expect("ingest telemetry batch A");
    });

    let schema_b = schema.clone();
    let telemetry_b = telemetry.clone();
    let handle_b = tokio::spawn(async move {
        ingest_b
            .batch_execute(&format!(
                "INSERT INTO {schema_b}.{telemetry_b} (id, device_id, temperature, battery_level, \
                 health) VALUES
                 ('t-4', 'dev-3', 66, 42, 'healthy'),
                 ('t-5', 'dev-3', 91, 39, 'degraded');"
            ))
            .await
            .expect("ingest telemetry batch B");
    });

    handle_a.await.expect("join ingest A");
    handle_b.await.expect("join ingest B");

    let device_risks = agent_pg
        .query(
            &format!(
                "SELECT device_id, MAX(temperature) AS max_temp, MIN(battery_level) AS min_battery
                 FROM {schema}.{telemetry}
                 GROUP BY device_id
                 HAVING MAX(temperature) >= 85 OR MIN(battery_level) <= 20
                 ORDER BY device_id"
            ),
            &[],
        )
        .await
        .expect("query risky devices");

    assert_eq!(device_risks.len(), 3, "three devices should trigger heat or battery workflows");

    for row in &device_risks {
        let device_id: &str = row.get(0);
        let max_temp: i32 = row.get(1);
        let min_battery: i32 = row.get(2);
        let action = if min_battery <= 20 {
            "dispatch-battery-tech"
        } else {
            "throttle-load"
        };
        let severity = if max_temp >= 90 || min_battery <= 20 {
            "critical"
        } else {
            "warning"
        };

        agent_pg
            .execute(
                &format!(
                    "INSERT INTO {schema}.{commands} (id, device_id, action, status, issued_by) \
                     VALUES ($1, $2, $3, $4, $5)"
                ),
                &[
                    &format!("cmd-{device_id}"),
                    &device_id,
                    &action,
                    &"queued",
                    &"fleet-agent",
                ],
            )
            .await
            .expect("insert remediation command");
        agent_pg
            .execute(
                &format!(
                    "INSERT INTO {schema}.{alerts} (id, device_id, severity, summary, created_by) \
                     VALUES ($1, $2, $3, $4, $5)"
                ),
                &[
                    &format!("alert-{device_id}"),
                    &device_id,
                    &severity,
                    &format!("temp={max_temp}, battery={min_battery}"),
                    &"fleet-agent",
                ],
            )
            .await
            .expect("insert incident alert");
    }

    let site_summary = operator_pg
        .query(
            &format!(
                "SELECT d.site_id, COUNT(c.id) AS command_count, COUNT(a.id) AS alert_count
                 FROM {schema}.{devices} AS d
                 LEFT JOIN {schema}.{commands} AS c ON c.device_id = d.id
                 LEFT JOIN {schema}.{alerts} AS a ON a.device_id = d.id
                 GROUP BY d.site_id
                 ORDER BY d.site_id"
            ),
            &[],
        )
        .await
        .expect("query site incident summary");

    assert_eq!(site_summary.len(), 2, "expected aggregated incident summary for two sites");
    let plant_a: (&str, i64, i64) =
        (site_summary[0].get(0), site_summary[0].get(1), site_summary[0].get(2));
    let plant_b: (&str, i64, i64) =
        (site_summary[1].get(0), site_summary[1].get(1), site_summary[1].get(2));
    assert_eq!(plant_a, ("plant-a", 2, 2));
    assert_eq!(plant_b, ("plant-b", 1, 1));

    let remote_commands = env
        .kalamdb_sql(&format!(
            "SELECT device_id, action, status FROM {schema}.{commands} ORDER BY device_id"
        ))
        .await;
    let remote_commands_text = serde_json::to_string(&remote_commands).unwrap_or_default();
    assert!(
        remote_commands_text.contains("dispatch-battery-tech")
            && remote_commands_text.contains("throttle-load"),
        "iot agent commands should be mirrored into KalamDB: {remote_commands_text}"
    );

    drop_kalam_tables(&pg_admin, &schema, &[devices, telemetry, commands, alerts]).await;
}
