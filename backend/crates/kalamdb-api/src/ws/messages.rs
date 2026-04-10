use actix_ws::Session;
use bytes::Bytes;
use flate2::read::GzDecoder;
use kalamdb_auth::UserRepository;
use kalamdb_commons::models::ConnectionInfo;
use kalamdb_commons::websocket::{ClientMessage, SerializationType};
use kalamdb_live::{LiveQueryManager, SharedConnectionState};
use std::io::Read;
use std::sync::Arc;

use super::events::{
    auth::{handle_authenticate, send_current_auth_success},
    batch::handle_next_batch,
    send_error,
    subscription::handle_subscribe,
    unsubscribe::handle_unsubscribe,
};
use super::models::WsErrorCode;
use crate::limiter::RateLimiter;

#[allow(clippy::too_many_arguments)]
pub(super) async fn handle_text_message(
    connection_state: &SharedConnectionState,
    client_ip: &ConnectionInfo,
    text: &str,
    session: &mut Session,
    app_context: &Arc<kalamdb_core::app_context::AppContext>,
    rate_limiter: &Arc<RateLimiter>,
    live_query_manager: &Arc<LiveQueryManager>,
    user_repo: &Arc<dyn UserRepository>,
    compression_enabled: bool,
) -> Result<(), String> {
    let msg: ClientMessage =
        serde_json::from_str(text).map_err(|e| format!("Invalid message: {}", e))?;

    handle_client_message(
        msg,
        connection_state,
        client_ip,
        session,
        app_context,
        rate_limiter,
        live_query_manager,
        user_repo,
        compression_enabled,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn handle_binary_message(
    connection_state: &SharedConnectionState,
    client_ip: &ConnectionInfo,
    data: Bytes,
    session: &mut Session,
    app_context: &Arc<kalamdb_core::app_context::AppContext>,
    rate_limiter: &Arc<RateLimiter>,
    live_query_manager: &Arc<LiveQueryManager>,
    user_repo: &Arc<dyn UserRepository>,
    compression_enabled: bool,
) -> Result<(), String> {
    match connection_state.serialization_type() {
        SerializationType::MessagePack => {
            let raw = if data.len() >= 2 && data[0] == 0x1f && data[1] == 0x8b {
                let mut decoder = GzDecoder::new(&data[..]);
                let mut buf = Vec::new();
                decoder
                    .read_to_end(&mut buf)
                    .map_err(|_| "Failed to decompress binary msgpack".to_string())?;
                buf
            } else {
                data.to_vec()
            };

            let client_msg = rmp_serde::from_slice::<ClientMessage>(&raw)
                .map_err(|e| format!("Invalid MessagePack: {}", e))?;

            handle_client_message(
                client_msg,
                connection_state,
                client_ip,
                session,
                app_context,
                rate_limiter,
                live_query_manager,
                user_repo,
                compression_enabled,
            )
            .await
        },
        SerializationType::Json => {
            let _ = send_error(
                session,
                "protocol",
                WsErrorCode::UnsupportedData,
                "Binary not supported",
                compression_enabled,
            )
            .await;
            Ok(())
        },
    }
}

#[allow(clippy::too_many_arguments)]
async fn handle_client_message(
    msg: ClientMessage,
    connection_state: &SharedConnectionState,
    client_ip: &ConnectionInfo,
    session: &mut Session,
    app_context: &Arc<kalamdb_core::app_context::AppContext>,
    rate_limiter: &Arc<RateLimiter>,
    live_query_manager: &Arc<LiveQueryManager>,
    user_repo: &Arc<dyn UserRepository>,
    compression_enabled: bool,
) -> Result<(), String> {
    match msg {
        ClientMessage::Authenticate {
            credentials,
            protocol,
        } => {
            if connection_state.is_authenticated() {
                return send_current_auth_success(connection_state, session, compression_enabled)
                    .await;
            }

            connection_state.mark_auth_started();
            handle_authenticate(
                connection_state,
                client_ip,
                credentials,
                protocol,
                session,
                app_context,
                rate_limiter,
                user_repo,
                compression_enabled,
            )
            .await
        },
        ClientMessage::Subscribe { subscription } => {
            if !connection_state.is_authenticated() {
                let _ = send_error(
                    session,
                    "subscribe",
                    WsErrorCode::AuthRequired,
                    "Authentication required before subscribing",
                    compression_enabled,
                )
                .await;
                return Ok(());
            }

            handle_subscribe(
                connection_state,
                subscription,
                session,
                rate_limiter,
                live_query_manager,
                compression_enabled,
            )
            .await
        },
        ClientMessage::NextBatch {
            subscription_id,
            last_seq_id,
        } => {
            if !connection_state.is_authenticated() {
                return Ok(());
            }

            handle_next_batch(
                connection_state,
                &subscription_id,
                last_seq_id,
                session,
                live_query_manager,
                compression_enabled,
            )
            .await
        },
        ClientMessage::Unsubscribe { subscription_id } => {
            if !connection_state.is_authenticated() {
                return Ok(());
            }

            handle_unsubscribe(connection_state, &subscription_id, rate_limiter, live_query_manager)
                .await
        },
        ClientMessage::Ping => Ok(()),
    }
}
