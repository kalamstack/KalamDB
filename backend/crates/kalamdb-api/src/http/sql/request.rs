use actix_multipart::Multipart;
use actix_web::{web, Either, FromRequest, HttpRequest, HttpResponse};
use std::time::Instant;

use super::file_utils::parse_sql_payload;
use super::models::{ErrorCode, ParsedSqlPayload, QueryRequest, SqlResponse};
use kalamdb_core::app_context::AppContext;

#[inline]
pub(super) fn took_ms(start_time: Instant) -> f64 {
    start_time.elapsed().as_secs_f64() * 1000.0
}

#[inline]
pub(super) fn is_multipart_content_type(content_type: &str) -> bool {
    const PREFIX: &[u8] = b"multipart/form-data";
    content_type.len() >= PREFIX.len()
        && content_type.as_bytes()[..PREFIX.len()].eq_ignore_ascii_case(PREFIX)
}

pub(super) async fn parse_incoming_payload(
    http_req: &HttpRequest,
    payload: web::Payload,
    app_context: &AppContext,
    start_time: Instant,
) -> Result<ParsedSqlPayload, HttpResponse> {
    let content_type = http_req
        .headers()
        .get(actix_web::http::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    let is_multipart = is_multipart_content_type(content_type);

    let mut payload = payload.into_inner();

    if is_multipart {
        let multipart = Multipart::new(http_req.headers(), payload);
        parse_sql_payload(Either::Right(multipart), &app_context.config().files)
            .await
            .map_err(|e| {
                HttpResponse::BadRequest().json(SqlResponse::error(
                    e.code,
                    &e.message,
                    took_ms(start_time),
                ))
            })
    } else {
        let json =
            web::Json::<QueryRequest>::from_request(http_req, &mut payload)
                .await
                .map_err(|e| {
                    HttpResponse::BadRequest().json(SqlResponse::error(
                        ErrorCode::InvalidInput,
                        &format!("Invalid JSON payload: {}", e),
                        took_ms(start_time),
                    ))
                })?;
        parse_sql_payload(Either::Left(json), &app_context.config().files)
            .await
            .map_err(|e| {
                HttpResponse::BadRequest().json(SqlResponse::error(
                    e.code,
                    &e.message,
                    took_ms(start_time),
                ))
            })
    }
}

#[inline]
pub(super) fn validate_sql_length(sql: &str, start_time: Instant) -> Result<(), HttpResponse> {
    let max = kalamdb_commons::constants::MAX_SQL_QUERY_LENGTH;
    if sql.len() > max {
        log::warn!("SQL query rejected: length {} bytes exceeds maximum {} bytes", sql.len(), max,);
        return Err(HttpResponse::BadRequest().json(SqlResponse::error(
            ErrorCode::InvalidSql,
            &format!("SQL query too long: {} bytes (maximum {} bytes)", sql.len(), max),
            took_ms(start_time),
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::is_multipart_content_type;

    #[test]
    fn multipart_detection_case_insensitive() {
        assert!(is_multipart_content_type("multipart/form-data"));
        assert!(is_multipart_content_type("Multipart/Form-Data; boundary=abc"));
        assert!(is_multipart_content_type("MULTIPART/FORM-DATA"));
        assert!(!is_multipart_content_type("application/json"));
        assert!(!is_multipart_content_type(""));
    }
}
