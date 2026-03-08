use std::sync::Arc;

use axum::{
    Router,
    routing::{delete, get, post},
};
use tower_http::cors::{Any, CorsLayer};

use crate::{api::handlers::*, client::pool::ClientPool};

pub fn build_router(pool: Arc<ClientPool>) -> Router {
    let health_route = Router::new().route("/health", get(health));
    let router = Router::new()
        .route("/groups", get(list_groups))
        .route("/groups/{name}", post(create_group))
        .route("/groups/{name}", delete(drop_group))
        .route("/groups/{name}/stats", get(group_stats))
        .route("/groups/{name}/entries", post(add_entry))
        .route("/groups/{name}/entries/batch", post(add_range_entries))
        .route("/groups/{name}/entries/single/{id}", get(read_entry))
        .route(
            "/groups/{name}/entries/range/{start_id}/{end_id}",
            get(read_range_entries),
        )
        .route(
            "/groups/{name}/entries/trim/{upto_id}",
            delete(drop_entries),
        )
        // kv
        .route("/kv/:db/:key", post(kv_set).get(kv_get).delete(kv_del))
        .route("/kv/:db/keys", get(kv_keys))
        .route("/kv/:db/flush", post(kv_flush))
        // list
        .route("/list/:key/push", post(list_push))
        .route("/list/:key/push-range", post(list_push_range))
        .route("/list/:key/pop", post(list_pop))
        .route("/list/:key/pop-range", post(list_pop_range))
        .route("/list/:key/pop-count", post(list_pop_count))
        .route("/list/:key/len", post(list_len))
        .route("/list/:key/flush", post(list_flush))
        .with_state(pool);
    // TODO: implement seg_list endpoints
    Router::new().merge(health_route).merge(router).layer(
        CorsLayer::new()
            .allow_origin(Any)
            .allow_methods(Any)
            .allow_headers(Any),
    )
}
