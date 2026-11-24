use super::*;

#[tokio::test]
async fn integrity_resets_persisted_checkpoint_after_restart() {
    let db_path = temp_db_path("dashboard-rollup-integrity-restart-checkpoint");
    let db_str = db_path.to_string_lossy().to_string();
    let proxy = TavilyProxy::with_endpoint(Vec::<String>::new(), DEFAULT_UPSTREAM, &db_str)
        .await
        .expect("create initial proxy");
    let now = proxy.backend_time().now_ts();
    let range_start =
        (now - 10 * SECS_PER_MINUTE).div_euclid(SECS_PER_FIVE_MINUTES) * SECS_PER_FIVE_MINUTES;
    let range_end = range_start + SECS_PER_FIVE_MINUTES;
    pin_integrity_hot_work(&proxy, range_start, range_end).await;
    for offset in 0..501_i64 {
        insert_visible_dashboard_log(
            &proxy,
            range_start + offset.rem_euclid(SECS_PER_FIVE_MINUTES),
        )
        .await;
    }
    let checkpointed = proxy
        .run_dashboard_rollup_integrity_slice()
        .await
        .expect("persist a source aggregation checkpoint");
    assert_eq!(checkpointed.state, "deferred");
    sqlx::query(
        "UPDATE request_logs SET business_credits = 9 WHERE created_at >= ? AND created_at < ?",
    )
    .bind(range_start)
    .bind(range_end)
    .execute(&proxy.key_store.pool)
    .await
    .expect("settle source credits during the hard-stop gap");
    proxy
        .shutdown_request_stats_coalescer(Duration::from_secs(2))
        .await
        .expect("stop initial proxy worker");
    drop(proxy);

    let restarted = TavilyProxy::with_endpoint(Vec::<String>::new(), DEFAULT_UPSTREAM, &db_str)
        .await
        .expect("restart proxy and reset persisted checkpoint");
    let first = restarted
        .run_dashboard_rollup_integrity_slice()
        .await
        .expect("restart source pagination");
    assert_eq!(first.state, "deferred");
    let repaired = restarted
        .run_dashboard_rollup_integrity_slice()
        .await
        .expect("repair from the refreshed source checkpoint");
    assert_eq!(repaired.state, "repaired");
    let credits: i64 = sqlx::query_scalar(
        "SELECT COALESCE(SUM(local_estimated_credits), 0) FROM dashboard_request_rollup_buckets WHERE bucket_secs = 60 AND bucket_start >= ? AND bucket_start < ?",
    )
    .bind(range_start)
    .bind(range_end)
    .fetch_one(&restarted.key_store.pool)
    .await
    .expect("read restarted repair credits");
    assert_eq!(credits, 501 * 9);
}

#[tokio::test]
async fn integrity_restart_drops_process_local_version_from_done_work_and_can_seal() {
    let db_path = temp_db_path("dashboard-rollup-integrity-restart-done-temp-version");
    let db_str = db_path.to_string_lossy().to_string();
    let proxy = TavilyProxy::with_endpoint(Vec::<String>::new(), DEFAULT_UPSTREAM, &db_str)
        .await
        .expect("create initial proxy");
    let now = proxy.backend_time().now_ts();
    let day_start = local_day_bucket_start_utc_ts(now - SECS_PER_DAY);
    let day_end = next_local_day_start_utc_ts(day_start);
    let target_range_start = day_start + 6 * SECS_PER_HOUR;
    let target_range_end = target_range_start + SECS_PER_FIVE_MINUTES;
    let final_range_start = day_end - SECS_PER_FIVE_MINUTES;

    insert_visible_dashboard_log(&proxy, target_range_start + 60).await;
    let mutation = proxy
        .key_store
        .request_stats_coalescer
        .begin_dashboard_rollup_source_mutation(target_range_start + 60);
    sqlx::query("UPDATE request_logs SET business_credits = 9 WHERE created_at = ?")
        .bind(target_range_start + 60)
        .execute(&proxy.key_store.pool)
        .await
        .expect("durably update the target source row");
    mutation.commit().await;

    let durable_source_version: i64 = sqlx::query_scalar(
        "SELECT COALESCE(SUM(revision), 0) FROM dashboard_rollup_source_revisions WHERE bucket_start >= ? AND bucket_start < ?",
    )
    .bind(target_range_start)
    .bind(target_range_end)
    .fetch_one(&proxy.key_store.pool)
    .await
    .expect("read the target durable source version");
    let process_local_source_version = proxy
        .key_store
        .request_stats_coalescer
        .dashboard_rollup_source_version(target_range_start, target_range_end)
        .await;
    assert!(
        process_local_source_version > 0,
        "the fixture must persist an in-process coalescer contribution"
    );
    let source_fence: i64 = sqlx::query_scalar("SELECT COALESCE(MAX(id), 0) FROM request_logs")
        .fetch_one(&proxy.key_store.pool)
        .await
        .expect("read the fixture source fence");

    for bucket_start in (day_start..day_end).step_by(SECS_PER_FIVE_MINUTES as usize) {
        if bucket_start == final_range_start {
            continue;
        }
        let bucket_durable_source_version: i64 = sqlx::query_scalar(
            "SELECT COALESCE(SUM(revision), 0) FROM dashboard_rollup_source_revisions WHERE bucket_start >= ? AND bucket_start < ?",
        )
        .bind(bucket_start)
        .bind(bucket_start + SECS_PER_FIVE_MINUTES)
        .fetch_one(&proxy.key_store.pool)
        .await
        .expect("read completed bucket source version");
        let bucket_source_version =
            bucket_durable_source_version.saturating_add(if bucket_start == target_range_start {
                process_local_source_version
            } else {
                0
            });
        sqlx::query(
            r#"
            INSERT INTO dashboard_rollup_integrity_work_items (
                range_start, range_end, source_fence, source_version, durable_source_version,
                cursor_created_at, cursor_id, counts_json, status, priority, updated_at
            ) VALUES (?, ?, ?, ?, ?, NULL, NULL, ?, 'done', 3, ?)
            "#,
        )
        .bind(bucket_start)
        .bind(bucket_start + SECS_PER_FIVE_MINUTES)
        .bind(source_fence)
        .bind(bucket_source_version)
        .bind(bucket_durable_source_version)
        .bind(if bucket_start == target_range_start {
            r#"{"total_requests":1}"#
        } else {
            "{}"
        })
        .bind(now)
        .execute(&proxy.key_store.pool)
        .await
        .expect("seed completed day work");
    }

    let pending_range_start = day_start - SECS_PER_HOUR;
    sqlx::query(
        r#"
        INSERT INTO dashboard_rollup_integrity_work_items (
            range_start, range_end, source_fence, source_version, durable_source_version,
            cursor_created_at, cursor_id, counts_json, status, priority, updated_at
        ) VALUES (?, ?, 0, 0, 0, ?, 999, '{"total_requests":7}', 'pending', 0, ?)
        "#,
    )
    .bind(pending_range_start)
    .bind(pending_range_start + SECS_PER_FIVE_MINUTES)
    .bind(pending_range_start + 60)
    .bind(now)
    .execute(&proxy.key_store.pool)
    .await
    .expect("seed pending work outside the completed day");
    pin_integrity_hot_work(&proxy, final_range_start, day_end).await;

    proxy
        .shutdown_request_stats_coalescer(Duration::from_secs(2))
        .await
        .expect("stop the initial proxy worker");
    drop(proxy);

    let restarted = TavilyProxy::with_endpoint(Vec::<String>::new(), DEFAULT_UPSTREAM, &db_str)
        .await
        .expect("restart proxy and normalize the completed checkpoint");
    let restored: (String, i64, i64, i64, String) = sqlx::query_as(
        r#"
        SELECT status, source_version, durable_source_version, source_fence, counts_json
        FROM dashboard_rollup_integrity_work_items
        WHERE range_start = ?
        "#,
    )
    .bind(target_range_start)
    .fetch_one(&restarted.key_store.pool)
    .await
    .expect("read the restored completed checkpoint");
    assert_eq!(
        restored,
        (
            "done".to_string(),
            durable_source_version,
            durable_source_version,
            source_fence,
            r#"{"total_requests":1}"#.to_string(),
        ),
        "restart must drop only the dead process-local contribution"
    );

    let pending_restored: (String, i64, i64, i64, Option<i64>, Option<i64>, String) =
        sqlx::query_as(
            r#"
            SELECT status, source_version, durable_source_version, source_fence,
                   cursor_created_at, cursor_id, counts_json
            FROM dashboard_rollup_integrity_work_items
            WHERE range_start = ?
            "#,
        )
        .bind(pending_range_start)
        .fetch_one(&restarted.key_store.pool)
        .await
        .expect("read the reset pending checkpoint");
    assert_eq!(
        pending_restored,
        (
            "pending".to_string(),
            0,
            0,
            source_fence,
            None,
            None,
            "{}".to_string(),
        ),
        "startup must retain and refresh ordinary pending work"
    );

    pin_integrity_hot_work(&restarted, final_range_start, day_end).await;
    let sealed = restarted
        .run_dashboard_rollup_integrity_slice()
        .await
        .expect("finish and seal the normalized day");
    assert!(matches!(sealed.state.as_str(), "verified" | "repaired"));
    let daily_seal: (i64, i64) = sqlx::query_as(
        "SELECT source_version, durable_source_version FROM dashboard_rollup_daily_seals WHERE bucket_start = ?",
    )
    .bind(day_start)
    .fetch_one(&restarted.key_store.pool)
    .await
    .expect("read the day seal after restart");
    assert_eq!(daily_seal, (durable_source_version, durable_source_version));
}

#[tokio::test]
async fn integrity_restart_keeps_done_work_stale_after_real_source_update_or_insert() {
    const STALE_PROCESS_LOCAL_VERSION: i64 = 7;
    let db_path = temp_db_path("dashboard-rollup-integrity-restart-source-change");
    let db_str = db_path.to_string_lossy().to_string();
    let proxy = TavilyProxy::with_endpoint(Vec::<String>::new(), DEFAULT_UPSTREAM, &db_str)
        .await
        .expect("create initial proxy");
    let now = proxy.backend_time().now_ts();
    let day_start = local_day_bucket_start_utc_ts(now - 2 * SECS_PER_DAY);
    let update_range_start = day_start + 6 * SECS_PER_HOUR;
    let update_range_end = update_range_start + SECS_PER_FIVE_MINUTES;
    let insert_range_start = update_range_end;
    let insert_range_end = insert_range_start + SECS_PER_FIVE_MINUTES;

    insert_visible_dashboard_log(&proxy, update_range_start + 60).await;
    let update_source_fence: i64 = sqlx::query_scalar(
        "SELECT MAX(id) FROM request_logs WHERE visibility = 'visible' AND created_at >= ? AND created_at < ?",
    )
    .bind(update_range_start)
    .bind(update_range_end)
    .fetch_one(&proxy.key_store.pool)
    .await
    .expect("read updated row source fence");
    let update_durable_source_version: i64 = sqlx::query_scalar(
        "SELECT COALESCE(SUM(revision), 0) FROM dashboard_rollup_source_revisions WHERE bucket_start >= ? AND bucket_start < ?",
    )
    .bind(update_range_start)
    .bind(update_range_end)
    .fetch_one(&proxy.key_store.pool)
    .await
    .expect("read updated row durable source version");
    sqlx::query(
        r#"
        INSERT INTO dashboard_rollup_integrity_work_items (
            range_start, range_end, source_fence, source_version, durable_source_version,
            cursor_created_at, cursor_id, counts_json, status, priority, updated_at
        ) VALUES (?, ?, ?, ?, ?, NULL, NULL, '{"total_requests":1}', 'done', 3, ?)
        "#,
    )
    .bind(update_range_start)
    .bind(update_range_end)
    .bind(update_source_fence)
    .bind(update_durable_source_version.saturating_add(STALE_PROCESS_LOCAL_VERSION))
    .bind(update_durable_source_version)
    .bind(now)
    .execute(&proxy.key_store.pool)
    .await
    .expect("seed the checkpoint before the source update");
    sqlx::query(
        r#"
        INSERT INTO dashboard_rollup_integrity_work_items (
            range_start, range_end, source_fence, source_version, durable_source_version,
            cursor_created_at, cursor_id, counts_json, status, priority, updated_at
        ) VALUES (?, ?, 0, ?, 0, NULL, NULL, '{}', 'done', 3, ?)
        "#,
    )
    .bind(insert_range_start)
    .bind(insert_range_end)
    .bind(STALE_PROCESS_LOCAL_VERSION)
    .bind(now)
    .execute(&proxy.key_store.pool)
    .await
    .expect("seed the checkpoint before the source insert");

    sqlx::query("UPDATE request_logs SET business_credits = 9 WHERE id = ?")
        .bind(update_source_fence)
        .execute(&proxy.key_store.pool)
        .await
        .expect("update the source after the checkpoint");
    insert_visible_dashboard_log(&proxy, insert_range_start + 60).await;
    let updated_durable_source_version: i64 = sqlx::query_scalar(
        "SELECT COALESCE(SUM(revision), 0) FROM dashboard_rollup_source_revisions WHERE bucket_start >= ? AND bucket_start < ?",
    )
    .bind(update_range_start)
    .bind(update_range_end)
    .fetch_one(&proxy.key_store.pool)
    .await
    .expect("read changed durable source version");
    let inserted_source_fence: i64 = sqlx::query_scalar(
        "SELECT MAX(id) FROM request_logs WHERE visibility = 'visible' AND created_at >= ? AND created_at < ?",
    )
    .bind(insert_range_start)
    .bind(insert_range_end)
    .fetch_one(&proxy.key_store.pool)
    .await
    .expect("read inserted source fence");
    assert!(updated_durable_source_version > update_durable_source_version);
    assert!(inserted_source_fence > 0);

    proxy
        .shutdown_request_stats_coalescer(Duration::from_secs(2))
        .await
        .expect("stop the initial proxy worker");
    drop(proxy);

    let restarted = TavilyProxy::with_endpoint(Vec::<String>::new(), DEFAULT_UPSTREAM, &db_str)
        .await
        .expect("restart proxy without laundering stale completed work");
    let updated_checkpoint: (String, i64, i64, i64, String) = sqlx::query_as(
        r#"
        SELECT status, source_version, durable_source_version, source_fence, counts_json
        FROM dashboard_rollup_integrity_work_items
        WHERE range_start = ?
        "#,
    )
    .bind(update_range_start)
    .fetch_one(&restarted.key_store.pool)
    .await
    .expect("read checkpoint after a real source update");
    assert_eq!(
        updated_checkpoint,
        (
            "done".to_string(),
            update_durable_source_version.saturating_add(STALE_PROCESS_LOCAL_VERSION),
            update_durable_source_version,
            update_source_fence,
            r#"{"total_requests":1}"#.to_string(),
        ),
        "a durable revision change must remain visible to the seal validator"
    );

    let inserted_checkpoint: (String, i64, i64, i64, String) = sqlx::query_as(
        r#"
        SELECT status, source_version, durable_source_version, source_fence, counts_json
        FROM dashboard_rollup_integrity_work_items
        WHERE range_start = ?
        "#,
    )
    .bind(insert_range_start)
    .fetch_one(&restarted.key_store.pool)
    .await
    .expect("read checkpoint after a real source insert");
    assert_eq!(
        inserted_checkpoint,
        (
            "done".to_string(),
            STALE_PROCESS_LOCAL_VERSION,
            0,
            0,
            "{}".to_string(),
        ),
        "a source insert inside the range must keep the old checkpoint stale"
    );
}

#[tokio::test]
async fn integrity_exclusive_offline_reopen_normalizes_done_without_resetting_pending() {
    let db_path = temp_db_path("dashboard-rollup-integrity-offline-done-normalization");
    let db_str = db_path.to_string_lossy().to_string();
    let store = crate::store::KeyStore::open_for_request_statistics_recovery_with_time(
        &db_str,
        BackendTime::system(),
    )
    .await
    .expect("create exclusive statistics recovery store");
    let now = store.backend_time.now_ts();
    let done_range_start =
        (now - 2 * SECS_PER_HOUR).div_euclid(SECS_PER_FIVE_MINUTES) * SECS_PER_FIVE_MINUTES;
    let pending_range_start = done_range_start + SECS_PER_FIVE_MINUTES;

    sqlx::query(
        r#"
        INSERT INTO dashboard_rollup_integrity_work_items (
            range_start, range_end, source_fence, source_version, durable_source_version,
            cursor_created_at, cursor_id, counts_json, status, priority, updated_at
        ) VALUES (?, ?, 0, 7, 0, NULL, NULL, '{}', 'done', 3, ?)
        "#,
    )
    .bind(done_range_start)
    .bind(done_range_start + SECS_PER_FIVE_MINUTES)
    .bind(now)
    .execute(&store.pool)
    .await
    .expect("seed completed work with a dead process-local version");
    sqlx::query(
        r#"
        INSERT INTO dashboard_rollup_integrity_work_items (
            range_start, range_end, source_fence, source_version, durable_source_version,
            cursor_created_at, cursor_id, counts_json, status, priority, updated_at
        ) VALUES (?, ?, 0, 7, 0, ?, 701, '{"total_requests":9}', 'pending', 0, ?)
        "#,
    )
    .bind(pending_range_start)
    .bind(pending_range_start + SECS_PER_FIVE_MINUTES)
    .bind(pending_range_start + 60)
    .bind(now)
    .execute(&store.pool)
    .await
    .expect("seed pending checkpoint");

    drop(store);

    let reopened = crate::store::KeyStore::open_for_request_statistics_recovery_with_time(
        &db_str,
        BackendTime::system(),
    )
    .await
    .expect("open exclusive statistics recovery store");
    let done: (i64, i64) = sqlx::query_as(
        "SELECT source_version, durable_source_version FROM dashboard_rollup_integrity_work_items WHERE range_start = ?",
    )
    .bind(done_range_start)
    .fetch_one(&reopened.pool)
    .await
    .expect("read normalized completed work");
    assert_eq!(done, (0, 0));

    let pending: (i64, i64, i64, Option<i64>, Option<i64>, String) = sqlx::query_as(
        r#"
        SELECT source_version, durable_source_version, source_fence,
               cursor_created_at, cursor_id, counts_json
        FROM dashboard_rollup_integrity_work_items
        WHERE range_start = ?
        "#,
    )
    .bind(pending_range_start)
    .fetch_one(&reopened.pool)
    .await
    .expect("read untouched pending checkpoint");
    assert_eq!(
        pending,
        (
            7,
            0,
            0,
            Some(pending_range_start + 60),
            Some(701),
            r#"{"total_requests":9}"#.to_string(),
        )
    );
}
