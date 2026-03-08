use std::{collections::HashMap, sync::Arc};

use axum::{
    Json,
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
};
use base64::Engine;

use serde_json::json;

use crate::client::pool::ClientPool;

fn encode_payload_b64(payload: &HashMap<String, serde_json::Value>) -> Vec<u8> {
    let json_str = serde_json::to_string(payload).unwrap();
    let b64 = base64::engine::general_purpose::STANDARD
        .encode(json_str)
        .into_bytes();
    zstd::encode_all(&b64[..], 3).unwrap()
}

fn decode_payload_b64(encoded: &[u8]) -> HashMap<String, serde_json::Value> {
    let decompressed = zstd::decode_all(encoded).unwrap();
    let b64_str = String::from_utf8(decompressed).unwrap();
    let json_str = base64::engine::general_purpose::STANDARD
        .decode(b64_str)
        .unwrap();
    serde_json::from_slice(&json_str).unwrap()
}

pub async fn create_group(
    State(pool): State<Arc<ClientPool>>,
    Path(name): Path<String>,
) -> impl IntoResponse {
    let mut conn = match pool.acquire().await {
        Ok(c) => c,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": e.to_string()})),
            )
                .into_response();
        }
    };
    match conn.conn().create_group(&name).await {
        Ok(Ok(())) => (StatusCode::OK, Json(json!({"status": "group created"}))).into_response(),
        Ok(Err(e)) => (StatusCode::BAD_REQUEST, Json(json!({"error": e}))).into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": e.to_string()})),
        )
            .into_response(),
    }
}

pub async fn list_groups(State(pool): State<Arc<ClientPool>>) -> impl IntoResponse {
    let mut conn = match pool.acquire().await {
        Ok(c) => c,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": e.to_string()})),
            )
                .into_response();
        }
    };
    match conn.conn().list_groups().await {
        Ok(Ok(groups)) => (StatusCode::OK, Json(json!({"groups": groups}))).into_response(),
        Ok(Err(e)) => (StatusCode::BAD_REQUEST, Json(json!({"error": e}))).into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": e.to_string()})),
        )
            .into_response(),
    }
}

pub async fn drop_group(
    State(pool): State<Arc<ClientPool>>,
    Path(name): Path<String>,
) -> impl IntoResponse {
    let mut conn = match pool.acquire().await {
        Ok(c) => c,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": e.to_string()})),
            )
                .into_response();
        }
    };
    match conn.conn().drop_group(&name).await {
        Ok(Ok(())) => (StatusCode::OK, Json(json!({"status": "group dropped"}))).into_response(),
        Ok(Err(e)) => (StatusCode::BAD_REQUEST, Json(json!({"error": e}))).into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": e.to_string()})),
        )
            .into_response(),
    }
}

pub async fn group_stats(
    State(pool): State<Arc<ClientPool>>,
    Path(name): Path<String>,
) -> impl IntoResponse {
    let mut conn = match pool.acquire().await {
        Ok(c) => c,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": e.to_string()})),
            )
                .into_response();
        }
    };
    match conn.conn().group_stats(&name).await {
        Ok(Ok((entries, segments, next_id))) => (
            StatusCode::OK,
            Json(json!({"entries": entries, "segments": segments, "next_id": next_id})),
        )
            .into_response(),
        Ok(Err(e)) => (StatusCode::BAD_REQUEST, Json(json!({"error": e}))).into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": e.to_string()})),
        )
            .into_response(),
    }
}

pub async fn add_entry(
    State(pool): State<Arc<ClientPool>>,
    Path(name): Path<String>,
    Json(req): Json<HashMap<String, serde_json::Value>>,
) -> impl IntoResponse {
    let mut conn = match pool.acquire().await {
        Ok(c) => c,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": e.to_string()})),
            )
                .into_response();
        }
    };
    let payload = encode_payload_b64(&req);
    let timestamp = chrono::Utc::now().timestamp_millis() as u64;
    match conn.conn().add(&name, timestamp, &payload).await {
        Ok(Ok(id)) => (
            StatusCode::OK,
            Json(json!({"status": "entry added", "id": id})),
        )
            .into_response(),
        Ok(Err(e)) => (StatusCode::BAD_REQUEST, Json(json!({"error": e}))).into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": e.to_string()})),
        )
            .into_response(),
    }
}

pub async fn add_range_entries(
    State(pool): State<Arc<ClientPool>>,
    Path(name): Path<String>,
    Json(req): Json<Vec<HashMap<String, serde_json::Value>>>,
) -> impl IntoResponse {
    let mut conn = match pool.acquire().await {
        Ok(c) => c,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": e.to_string()})),
            )
                .into_response();
        }
    };
    let entries: Vec<(u64, Vec<u8>)> = req
        .into_iter()
        .map(|e| {
            (
                chrono::Utc::now().timestamp_millis() as u64,
                encode_payload_b64(&e),
            )
        })
        .collect();
    let entries_ref: Vec<(u64, &[u8])> = entries
        .iter()
        .map(|(ts, payload)| (*ts, payload.as_slice()))
        .collect();
    match conn.conn().add_range(&name, &entries_ref).await {
        Ok(Ok((first, last))) => (
            StatusCode::OK,
            Json(json!({"status": "entries added", "first": first, "last": last})),
        )
            .into_response(),
        Ok(Err(e)) => (StatusCode::BAD_REQUEST, Json(json!({"error": e}))).into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": e.to_string()})),
        )
            .into_response(),
    }
}

pub async fn read_entry(
    State(pool): State<Arc<ClientPool>>,
    Path((name, id)): Path<(String, u64)>,
) -> impl IntoResponse {
    let mut conn = match pool.acquire().await {
        Ok(c) => c,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": e.to_string()})),
            )
                .into_response();
        }
    };
    match conn.conn().read(&name, id).await {
        Ok(Ok((id, timestamp, payload))) => {
            let payload_decoded = decode_payload_b64(&payload);
            let utc_dt = chrono::DateTime::<chrono::Utc>::from_timestamp_millis(timestamp as i64);
            let tehran_tz = chrono_tz::Asia::Tehran;
            let time = match utc_dt {
                Some(dt) => dt
                    .with_timezone(&tehran_tz)
                    .format("%Y-%m-%d %H:%M:%S")
                    .to_string(),
                None => "invalid timestamp".to_string(),
            };
            (
                StatusCode::OK,
                Json(json!({"id": id, "timestamp": time, "payload": payload_decoded})),
            )
                .into_response()
        }
        Ok(Err(e)) => (StatusCode::BAD_REQUEST, Json(json!({"error": e}))).into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": e.to_string()})),
        )
            .into_response(),
    }
}

pub async fn read_range_entries(
    State(pool): State<Arc<ClientPool>>,
    Path((name, start_id, end_id)): Path<(String, u64, u64)>,
) -> impl IntoResponse {
    let mut conn = match pool.acquire().await {
        Ok(c) => c,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": e.to_string()})),
            )
                .into_response();
        }
    };
    match conn.conn().read_range(&name, start_id, end_id).await {
        Ok(Ok(entries)) => {
            let entries_decoded: Vec<_> = entries
                .into_iter()
                .map(|(id, timestamp, payload)| {
                    let payload_decoded = decode_payload_b64(&payload);
                    json!({"id": id, "timestamp": timestamp, "payload": payload_decoded})
                })
                .collect();
            (StatusCode::OK, Json(json!({"entries": entries_decoded}))).into_response()
        }
        Ok(Err(e)) => (StatusCode::BAD_REQUEST, Json(json!({"error": e}))).into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": e.to_string()})),
        )
            .into_response(),
    }
}

pub async fn drop_entries(
    State(pool): State<Arc<ClientPool>>,
    Path((group, upto_id)): Path<(String, u64)>,
) -> impl IntoResponse {
    let mut conn = match pool.acquire().await {
        Ok(c) => c,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": e.to_string()})),
            )
                .into_response();
        }
    };
    match conn.conn().remove(&group, upto_id).await {
        Ok(Ok(())) => (
            StatusCode::OK,
            Json(json!({"status": format!("entries dropped up to id {}", upto_id)})),
        )
            .into_response(),
        Ok(Err(e)) => (StatusCode::BAD_REQUEST, Json(json!({"error": e}))).into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": e.to_string()})),
        )
            .into_response(),
    }
}

pub async fn kv_set(
    State(pool): State<Arc<ClientPool>>,
    Path((db, key)): Path<(String, String)>,
    Json(req): Json<serde_json::Value>,
) -> impl IntoResponse {
    let mut conn = match pool.acquire().await {
        Ok(c) => c,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": e.to_string()})),
            )
                .into_response();
        }
    };
    let ttl_secs = req.get("ttl").and_then(|v| v.as_u64()).unwrap_or(0);
    let val = match req.get("val") {
        Some(v) => serde_json::to_vec(v).unwrap(),
        None => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({"error": "missing val"})),
            )
                .into_response();
        }
    };
    match conn.conn().kv_set(&db, &key, &val, ttl_secs).await {
        Ok(Ok(())) => (StatusCode::OK, Json(json!({"status": "ok"}))).into_response(),
        Ok(Err(e)) => (StatusCode::BAD_REQUEST, Json(json!({"error": e}))).into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": e.to_string()})),
        )
            .into_response(),
    }
}

pub async fn kv_get(
    State(pool): State<Arc<ClientPool>>,
    Path((db, key)): Path<(String, String)>,
) -> impl IntoResponse {
    let mut conn = match pool.acquire().await {
        Ok(c) => c,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": e.to_string()})),
            )
                .into_response();
        }
    };
    match conn.conn().kv_get(&db, &key).await {
        Ok(Ok(Some(bytes))) => {
            let val: serde_json::Value =
                serde_json::from_slice(&bytes).unwrap_or(serde_json::Value::Null);
            (StatusCode::OK, Json(json!({"val": val}))).into_response()
        }
        Ok(Ok(None)) => (
            StatusCode::NOT_FOUND,
            Json(json!({"error": "key not found"})),
        )
            .into_response(),
        Ok(Err(e)) => (StatusCode::BAD_REQUEST, Json(json!({"error": e}))).into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": e.to_string()})),
        )
            .into_response(),
    }
}

pub async fn kv_del(
    State(pool): State<Arc<ClientPool>>,
    Path((db, key)): Path<(String, String)>,
) -> impl IntoResponse {
    let mut conn = match pool.acquire().await {
        Ok(c) => c,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": e.to_string()})),
            )
                .into_response();
        }
    };
    match conn.conn().kv_del(&db, &key).await {
        Ok(Ok(())) => (StatusCode::OK, Json(json!({"status": "deleted"}))).into_response(),
        Ok(Err(e)) => (StatusCode::BAD_REQUEST, Json(json!({"error": e}))).into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": e.to_string()})),
        )
            .into_response(),
    }
}

pub async fn kv_keys(
    State(pool): State<Arc<ClientPool>>,
    Path(db): Path<String>,
) -> impl IntoResponse {
    let mut conn = match pool.acquire().await {
        Ok(c) => c,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": e.to_string()})),
            )
                .into_response();
        }
    };
    match conn.conn().kv_keys(&db).await {
        Ok(Ok(keys)) => (StatusCode::OK, Json(json!({"keys": keys}))).into_response(),
        Ok(Err(e)) => (StatusCode::BAD_REQUEST, Json(json!({"error": e}))).into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": e.to_string()})),
        )
            .into_response(),
    }
}

pub async fn kv_flush(
    State(pool): State<Arc<ClientPool>>,
    Path(db): Path<String>,
) -> impl IntoResponse {
    let mut conn = match pool.acquire().await {
        Ok(c) => c,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": e.to_string()})),
            )
                .into_response();
        }
    };
    match conn.conn().kv_flush(&db).await {
        Ok(Ok(())) => (StatusCode::OK, Json(json!({"status": "flushed"}))).into_response(),
        Ok(Err(e)) => (StatusCode::BAD_REQUEST, Json(json!({"error": e}))).into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": e.to_string()})),
        )
            .into_response(),
    }
}

pub async fn list_push(
    State(pool): State<Arc<ClientPool>>,
    Path(key): Path<String>,
    Json(req): Json<serde_json::Value>,
) -> impl IntoResponse {
    let mut conn = match pool.acquire().await {
        Ok(c) => c,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": e.to_string()})),
            )
                .into_response();
        }
    };
    let val = match req.get("val").and_then(|v| v.as_str()) {
        Some(v) => v.as_bytes().to_vec(),
        None => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({"error": "missing val"})),
            )
                .into_response();
        }
    };
    match conn.conn().l_push(&key, &val).await {
        Ok(Ok(())) => (StatusCode::OK, Json(json!({"status": "ok"}))).into_response(),
        Ok(Err(e)) => (StatusCode::BAD_REQUEST, Json(json!({"error": e}))).into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": e.to_string()})),
        )
            .into_response(),
    }
}

pub async fn list_push_range(
    State(pool): State<Arc<ClientPool>>,
    Path(key): Path<String>,
    Json(req): Json<serde_json::Value>,
) -> impl IntoResponse {
    let mut conn = match pool.acquire().await {
        Ok(c) => c,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": e.to_string()})),
            )
                .into_response();
        }
    };
    let vals_str: Vec<String> = match req
        .get("vals")
        .and_then(|v| serde_json::from_value(v.clone()).ok())
    {
        Some(v) => v,
        None => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({"error": "missing vals"})),
            )
                .into_response();
        }
    };
    let vals_bytes: Vec<Vec<u8>> = vals_str.iter().map(|s| s.as_bytes().to_vec()).collect();
    let vals: Vec<&[u8]> = vals_bytes.iter().map(|v| v.as_slice()).collect();
    match conn.conn().l_push_range(&key, &vals).await {
        Ok(Ok(())) => (StatusCode::OK, Json(json!({"status": "ok"}))).into_response(),
        Ok(Err(e)) => (StatusCode::BAD_REQUEST, Json(json!({"error": e}))).into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": e.to_string()})),
        )
            .into_response(),
    }
}

pub async fn list_pop(
    State(pool): State<Arc<ClientPool>>,
    Path(key): Path<String>,
) -> impl IntoResponse {
    let mut conn = match pool.acquire().await {
        Ok(c) => c,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": e.to_string()})),
            )
                .into_response();
        }
    };
    match conn.conn().l_pop(&key).await {
        Ok(Ok(Some(val))) => {
            let val_str = String::from_utf8_lossy(&val).to_string();
            (StatusCode::OK, Json(json!({"val": val_str}))).into_response()
        }
        Ok(Ok(None)) => (
            StatusCode::NOT_FOUND,
            Json(json!({"error": "list empty or not found"})),
        )
            .into_response(),
        Ok(Err(e)) => (StatusCode::BAD_REQUEST, Json(json!({"error": e}))).into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": e.to_string()})),
        )
            .into_response(),
    }
}

pub async fn list_pop_range(
    State(pool): State<Arc<ClientPool>>,
    Path(key): Path<String>,
    Json(req): Json<serde_json::Value>,
) -> impl IntoResponse {
    let mut conn = match pool.acquire().await {
        Ok(c) => c,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": e.to_string()})),
            )
                .into_response();
        }
    };
    let start = match req.get("start").and_then(|v| v.as_u64()) {
        Some(v) => v as u32,
        None => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({"error": "missing start"})),
            )
                .into_response();
        }
    };
    let end = match req.get("end").and_then(|v| v.as_u64()) {
        Some(v) => v as u32,
        None => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({"error": "missing end"})),
            )
                .into_response();
        }
    };
    match conn.conn().l_pop_range(&key, start, end).await {
        Ok(Ok(vals)) => {
            let decoded: Vec<String> = vals
                .iter()
                .map(|v| String::from_utf8_lossy(v).to_string())
                .collect();
            (StatusCode::OK, Json(json!({"vals": decoded}))).into_response()
        }
        Ok(Err(e)) => (StatusCode::BAD_REQUEST, Json(json!({"error": e}))).into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": e.to_string()})),
        )
            .into_response(),
    }
}

pub async fn list_pop_count(
    State(pool): State<Arc<ClientPool>>,
    Path(key): Path<String>,
    Json(req): Json<serde_json::Value>,
) -> impl IntoResponse {
    let mut conn = match pool.acquire().await {
        Ok(c) => c,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": e.to_string()})),
            )
                .into_response();
        }
    };
    let count = match req.get("count").and_then(|v| v.as_u64()) {
        Some(v) => v as u32,
        None => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({"error": "missing count"})),
            )
                .into_response();
        }
    };
    match conn.conn().l_pop_count(&key, count).await {
        Ok(Ok(vals)) => {
            let decoded: Vec<String> = vals
                .iter()
                .map(|v| String::from_utf8_lossy(v).to_string())
                .collect();
            (StatusCode::OK, Json(json!({"vals": decoded}))).into_response()
        }
        Ok(Err(e)) => (StatusCode::BAD_REQUEST, Json(json!({"error": e}))).into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": e.to_string()})),
        )
            .into_response(),
    }
}

pub async fn list_len(
    State(pool): State<Arc<ClientPool>>,
    Path(key): Path<String>,
) -> impl IntoResponse {
    let mut conn = match pool.acquire().await {
        Ok(c) => c,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": e.to_string()})),
            )
                .into_response();
        }
    };
    match conn.conn().l_len(&key).await {
        Ok(Ok(len)) => (StatusCode::OK, Json(json!({"len": len}))).into_response(),
        Ok(Err(e)) => (StatusCode::BAD_REQUEST, Json(json!({"error": e}))).into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": e.to_string()})),
        )
            .into_response(),
    }
}

pub async fn list_flush(
    State(pool): State<Arc<ClientPool>>,
    Path(key): Path<String>,
) -> impl IntoResponse {
    let mut conn = match pool.acquire().await {
        Ok(c) => c,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": e.to_string()})),
            )
                .into_response();
        }
    };
    match conn.conn().l_flush(&key).await {
        Ok(Ok(())) => (StatusCode::OK, Json(json!({"status": "flushed"}))).into_response(),
        Ok(Err(e)) => (StatusCode::BAD_REQUEST, Json(json!({"error": e}))).into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": e.to_string()})),
        )
            .into_response(),
    }
}

pub async fn health() -> impl IntoResponse {
    (StatusCode::OK, Json(json!({"status": "ok"})))
}
