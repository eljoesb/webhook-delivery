use axum::routing::{get, post};
use axum::{Json, Router};
use serde::Serialize;
use webhook_delivery::endpoints::{AppState, create_endpoint, get_endpoint, list_endpoints};
use webhook_delivery::events::{create_event, get_event, list_attempts};
use webhook_delivery::worker;

#[derive(Serialize)]
struct Health {
    status: &'static str,
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt().with_env_filter("info").init();

    let database_url = std::env::var("DATABASE_URL")
        .unwrap_or("postgres://postgres:postgres@localhost:5432/webhooks".to_string());

    let pool = sqlx::PgPool::connect(&database_url)
        .await
        .expect("failed to connect to postgres");
    sqlx::migrate!("./migrations")
        .run(&pool)
        .await
        .expect("failed to run migrations");

    let config = worker::WorkerConfig::from_env();
    let client = reqwest::Client::builder()
        .timeout(config.request_timeout)
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .expect("failed to build client");

    worker::spawn(pool.clone(), client, config);

    let app = Router::new()
        .route("/health", get(health))
        .route("/endpoints", post(create_endpoint).get(list_endpoints))
        .route("/endpoints/{id}", get(get_endpoint))
        .route("/endpoints/{id}/events", post(create_event))
        .route("/events/{id}", get(get_event))
        .route("/events/{id}/attempts", get(list_attempts))
        .with_state(AppState::new(pool));

    let listener = tokio::net::TcpListener::bind("127.0.0.1:3000")
        .await
        .unwrap();

    tracing::info!("Server is running on http://127.0.0.1:3000");
    axum::serve(listener, app).await.unwrap();
}

async fn health() -> Json<Health> {
    Json(Health { status: "ok" })
}
