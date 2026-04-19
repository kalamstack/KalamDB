use base64::Engine;
use wasm_bindgen::prelude::*;

use link_common::models::{AckResponse, ConsumeMessage, ConsumeResponse, RowData, UserId};

use crate::helpers::{
    fetch_json_response, js_value_to_json_string, response_text, serialize_json_to_js_value,
    topic_request_error,
};

#[wasm_bindgen]
pub struct KalamConsumerClient {
    url: String,
}

#[wasm_bindgen]
impl KalamConsumerClient {
    #[wasm_bindgen(constructor)]
    pub fn new(url: String) -> Result<KalamConsumerClient, JsValue> {
        if !url.starts_with("http://") && !url.starts_with("https://") {
            return Err(JsValue::from_str("Base URL must start with http:// or https://"));
        }

        Ok(Self {
            url: url.trim_end_matches('/').to_string(),
        })
    }

    pub async fn consume(
        &self,
        auth_header: Option<String>,
        request: JsValue,
    ) -> Result<JsValue, JsValue> {
        let body = js_value_to_json_string(&request, "Consume request")?;
        let request_value: serde_json::Value = serde_json::from_str(&body)
            .map_err(|error| JsValue::from_str(&format!("Invalid consume request: {}", error)))?;

        let response = fetch_json_response(
            &format!("{}/v1/api/topics/consume", self.url),
            &body,
            auth_header.as_deref(),
        )
        .await?;
        let status = response.status();
        let text = response_text(&response).await?;
        if !response.ok() {
            return Err(topic_request_error(status, &text, "Consume failed"));
        }

        let topic = request_value
            .get("topic_id")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default();
        let group_id = request_value
            .get("group_id")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default();
        let fallback_partition = request_value
            .get("partition_id")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(0) as u32;
        let response = decode_consume_response(&text, topic, group_id, fallback_partition)?;

        serialize_json_to_js_value(&response, "consume response")
    }

    pub async fn ack(
        &self,
        auth_header: Option<String>,
        request: JsValue,
    ) -> Result<JsValue, JsValue> {
        let body = js_value_to_json_string(&request, "Ack request")?;
        let request_value: serde_json::Value = serde_json::from_str(&body)
            .map_err(|error| JsValue::from_str(&format!("Invalid ack request: {}", error)))?;

        let response = fetch_json_response(
            &format!("{}/v1/api/topics/ack", self.url),
            &body,
            auth_header.as_deref(),
        )
        .await?;
        let status = response.status();
        let text = response_text(&response).await?;
        if !response.ok() {
            return Err(topic_request_error(status, &text, "Ack failed"));
        }

        let raw: serde_json::Value = serde_json::from_str(&text).map_err(|error| {
            JsValue::from_str(&format!("Failed to parse ack response: {}", error))
        })?;
        let fallback_offset = request_value
            .get("upto_offset")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(0);
        let response = AckResponse {
            success: raw.get("success").and_then(serde_json::Value::as_bool).unwrap_or(true),
            acknowledged_offset: raw
                .get("acknowledged_offset")
                .and_then(serde_json::Value::as_u64)
                .unwrap_or(fallback_offset),
        };

        serialize_json_to_js_value(&response, "ack response")
    }
}

fn decode_consume_response(
    text: &str,
    topic: &str,
    group_id: &str,
    fallback_partition: u32,
) -> Result<ConsumeResponse, JsValue> {
    let raw: serde_json::Value = serde_json::from_str(text).map_err(|error| {
        JsValue::from_str(&format!("Failed to parse consume response: {}", error))
    })?;
    let messages = raw
        .get("messages")
        .and_then(serde_json::Value::as_array)
        .map(|messages| {
            messages
                .iter()
                .map(|message| decode_consume_message(message, topic, group_id, fallback_partition))
                .collect()
        })
        .unwrap_or_default();

    Ok(ConsumeResponse {
        messages,
        next_offset: raw.get("next_offset").and_then(serde_json::Value::as_u64).unwrap_or(0),
        has_more: raw.get("has_more").and_then(serde_json::Value::as_bool).unwrap_or(false),
    })
}

fn decode_consume_message(
    raw: &serde_json::Value,
    topic: &str,
    group_id: &str,
    fallback_partition: u32,
) -> ConsumeMessage {
    ConsumeMessage {
        key: raw
            .get("key")
            .and_then(serde_json::Value::as_str)
            .map(ToOwned::to_owned)
            .or_else(|| raw.get("message_id").and_then(serde_json::Value::as_str).map(ToOwned::to_owned)),
        op: raw.get("op").and_then(serde_json::Value::as_str).map(ToOwned::to_owned),
        timestamp_ms: raw
            .get("timestamp_ms")
            .and_then(serde_json::Value::as_u64)
            .or_else(|| raw.get("ts").and_then(serde_json::Value::as_u64)),
        offset: raw.get("offset").and_then(serde_json::Value::as_u64).unwrap_or(0),
        partition_id: raw
            .get("partition_id")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(u64::from(fallback_partition)) as u32,
        topic: raw
            .get("topic_id")
            .and_then(serde_json::Value::as_str)
            .unwrap_or(topic)
            .to_string(),
        group_id: group_id.to_string(),
        user: raw
            .get("user")
            .or_else(|| raw.get("username"))
            .and_then(serde_json::Value::as_str)
            .map(UserId::from),
        payload: decode_payload_value(raw.get("payload").or_else(|| raw.get("value"))),
    }
}

fn decode_payload_value(payload: Option<&serde_json::Value>) -> RowData {
    let value = match payload {
        Some(serde_json::Value::String(payload)) => {
            match base64::engine::general_purpose::STANDARD.decode(payload) {
                Ok(bytes) => serde_json::from_slice(&bytes)
                    .unwrap_or_else(|_| serde_json::Value::String(payload.clone())),
                Err(_) => serde_json::Value::String(payload.clone()),
            }
        },
        Some(value @ serde_json::Value::Object(_)) => value.clone(),
        Some(other) => other.clone(),
        None => serde_json::Value::Null,
    };

    serde_json::from_value(value).unwrap_or_default()
}
