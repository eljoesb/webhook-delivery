use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::Router;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::routing::post;
use serde_json::json;
use uuid::Uuid;
use webhook_delivery::events::EventStatus;
use webhook_delivery::signature::sign;
use webhook_delivery::worker::{
    MAX_RESPONSE_CHARS, WorkerConfig, backoff, claim_event, decide, deliver, process_available,
    truncate,
};

const TEST_DATABASE_URL: &str = "postgres://postgres:postgres@localhost:5432/webhooks_test";
const ADMIN_DATABASE_URL: &str = "postgres://postgres:postgres@localhost:5432/postgres";

fn db_lock() -> &'static tokio::sync::Mutex<()> {
    static LOCK: std::sync::OnceLock<tokio::sync::Mutex<()>> = std::sync::OnceLock::new();
    LOCK.get_or_init(|| tokio::sync::Mutex::new(()))
}

#[test]
fn response_text_is_truncated_on_char_boundaries() {
    let text = "á".repeat(MAX_RESPONSE_CHARS + 10);
    let truncated = truncate(&text);
    assert_eq!(truncated.chars().count(), MAX_RESPONSE_CHARS);
}

#[derive(Clone)]
struct Receiver {
    hits: Arc<Mutex<Vec<Recorded>>>,
    statuses: Arc<Mutex<Vec<StatusCode>>>,
}

struct Recorded {
    signature: String,
    event_id: String,
    event_type: String,
    attempt: String,
    body: String,
}

async fn receive(State(receiver): State<Receiver>, headers: HeaderMap, body: String) -> StatusCode {
    receiver
        .hits
        .lock()
        .expect("receiver poisoned")
        .push(Recorded {
            signature: header(&headers, "x-webhook-signature"),
            event_id: header(&headers, "x-webhook-id"),
            event_type: header(&headers, "x-webhook-event"),
            attempt: header(&headers, "x-webhook-attempt"),
            body,
        });
    let mut statuses = receiver.statuses.lock().expect("receiver poisoned");
    if statuses.len() > 1 {
        statuses.remove(0)
    } else {
        statuses
            .first()
            .copied()
            .unwrap_or(StatusCode::INTERNAL_SERVER_ERROR)
    }
}

fn header(headers: &HeaderMap, name: &str) -> String {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .to_owned()
}

async fn test_pool() -> sqlx::PgPool {
    static READY: tokio::sync::OnceCell<()> = tokio::sync::OnceCell::const_new();
    READY
        .get_or_init(|| async {
            if sqlx::PgPool::connect(TEST_DATABASE_URL).await.is_err() {
                let admin = sqlx::PgPool::connect(ADMIN_DATABASE_URL)
                    .await
                    .expect("postgres must be running for delivery tests");
                sqlx::query("CREATE DATABASE webhooks_test")
                    .execute(&admin)
                    .await
                    .expect("create webhooks_test");
                admin.close().await;
            }
        })
        .await;
    let pool = sqlx::PgPool::connect(TEST_DATABASE_URL)
        .await
        .expect("connect to webhooks_test");
    sqlx::migrate!("./migrations")
        .run(&pool)
        .await
        .expect("migrations");
    pool
}

struct Snapshot {
    status: String,
    attempt_count: i32,
    next_attempt_at: chrono::DateTime<chrono::Utc>,
    attempts: Vec<(i32, bool)>,
}

struct Delivery {
    pool: sqlx::PgPool,
    endpoint_id: Uuid,
    event_id: Uuid,
    receiver: Receiver,
    closed: bool,
}

impl Delivery {
    async fn open(statuses: Vec<StatusCode>, secret: &str, payload: serde_json::Value) -> Self {
        let pool = test_pool().await;
        let (url, receiver) = spawn_receiver(statuses).await;
        let (endpoint_id, event_id) = insert_pending(&pool, &url, secret, payload).await;
        Self {
            pool,
            endpoint_id,
            event_id,
            receiver,
            closed: false,
        }
    }

    async fn snapshot(&self) -> Snapshot {
        let status = sqlx::query_scalar("SELECT status FROM events WHERE id = $1")
            .bind(self.event_id)
            .fetch_one(&self.pool)
            .await
            .unwrap();
        let attempt_count = sqlx::query_scalar("SELECT attempt_count FROM events WHERE id = $1")
            .bind(self.event_id)
            .fetch_one(&self.pool)
            .await
            .unwrap();
        let next_attempt_at =
            sqlx::query_scalar("SELECT next_attempt_at FROM events WHERE id = $1")
                .bind(self.event_id)
                .fetch_one(&self.pool)
                .await
                .unwrap();
        let attempts = sqlx::query_as(
            "SELECT attempt_number, succeeded FROM delivery_attempts WHERE event_id = $1 ORDER BY attempt_number",
        )
        .bind(self.event_id)
        .fetch_all(&self.pool)
        .await
        .unwrap();
        Snapshot {
            status,
            attempt_count,
            next_attempt_at,
            attempts,
        }
    }

    async fn make_due(&self) {
        sqlx::query("UPDATE events SET next_attempt_at = now() WHERE id = $1")
            .bind(self.event_id)
            .execute(&self.pool)
            .await
            .unwrap();
    }

    async fn close(mut self) {
        cleanup(&self.pool, self.endpoint_id, self.event_id).await;
        self.closed = true;
    }
}

impl Drop for Delivery {
    fn drop(&mut self) {
        if self.closed {
            return;
        }
        let endpoint_id = self.endpoint_id;
        let event_id = self.event_id;
        std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("cleanup runtime");
            runtime.block_on(async move {
                let pool = sqlx::PgPool::connect(TEST_DATABASE_URL)
                    .await
                    .expect("cleanup connection");
                cleanup(&pool, endpoint_id, event_id).await;
                pool.close().await;
            });
        })
        .join()
        .ok();
    }
}

async fn spawn_receiver(statuses: Vec<StatusCode>) -> (String, Receiver) {
    let receiver = Receiver {
        hits: Arc::new(Mutex::new(Vec::new())),
        statuses: Arc::new(Mutex::new(statuses)),
    };

    let app = Router::new()
        .route("/hook", post(receive))
        .with_state(receiver.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    (format!("http://{address}/hook"), receiver)
}

async fn insert_pending(
    pool: &sqlx::PgPool,
    url: &str,
    secret: &str,
    payload: serde_json::Value,
) -> (Uuid, Uuid) {
    let endpoint_id = Uuid::new_v4();
    let event_id = Uuid::new_v4();
    sqlx::query("INSERT INTO endpoints (id, name, url, secret) VALUES ($1, $2, $3, $4)")
        .bind(endpoint_id)
        .bind(format!("worker-test-{endpoint_id}"))
        .bind(url)
        .bind(secret)
        .execute(pool)
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO events (id, endpoint_id, event_type, payload) VALUES ($1, $2, 'invoice.paid', $3)",
    )
    .bind(event_id)
    .bind(endpoint_id)
    .bind(payload)
    .execute(pool)
    .await
    .unwrap();
    (endpoint_id, event_id)
}

async fn cleanup(pool: &sqlx::PgPool, endpoint_id: Uuid, event_id: Uuid) {
    sqlx::query("DELETE FROM delivery_attempts WHERE event_id = $1")
        .bind(event_id)
        .execute(pool)
        .await
        .unwrap();
    sqlx::query("DELETE FROM events WHERE id = $1")
        .bind(event_id)
        .execute(pool)
        .await
        .unwrap();
    sqlx::query("DELETE FROM endpoints WHERE id = $1")
        .bind(endpoint_id)
        .execute(pool)
        .await
        .unwrap();
}

async fn deliver_once(
    pool: &sqlx::PgPool,
    event_id: Uuid,
    max_attempts: i32,
) -> Result<(), sqlx::Error> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap();
    let config = WorkerConfig {
        lease: Duration::from_secs(30),
        poll_interval: Duration::from_secs(1),
        request_timeout: Duration::from_secs(5),
        max_attempts,
    };
    let event = claim_event(pool, event_id, config.lease)
        .await?
        .expect("event is claimable");
    deliver(pool, &client, &config, event).await
}

#[tokio::test]
async fn successful_delivery_is_recorded() {
    let _guard = db_lock().lock().await;
    let secret = "test-secret";
    let payload = json!({"invoice_id": 10});
    let delivery = Delivery::open(vec![StatusCode::OK], secret, payload.clone()).await;

    let result = deliver_once(&delivery.pool, delivery.event_id, 5).await;
    let snapshot = delivery.snapshot().await;
    let recorded = delivery.receiver.hits.lock().unwrap().remove(0);
    let event_id = delivery.event_id;
    delivery.close().await;

    result.unwrap();
    assert_eq!(snapshot.status, "delivered");
    assert_eq!(snapshot.attempts, vec![(1, true)]);
    assert_eq!(recorded.event_id, event_id.to_string());
    assert_eq!(recorded.event_type, "invoice.paid");
    assert_eq!(recorded.attempt, "1");
    assert_eq!(recorded.signature, sign(secret, recorded.body.as_bytes()));
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&recorded.body).unwrap(),
        payload
    );
}

#[tokio::test]
async fn process_available_delivers_one_due_event() {
    let _guard = db_lock().lock().await;
    let delivery = Delivery::open(vec![StatusCode::OK], "test-secret", json!({"n": 1})).await;
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap();
    let config = WorkerConfig {
        lease: Duration::from_secs(30),
        poll_interval: Duration::from_secs(1),
        request_timeout: Duration::from_secs(5),
        max_attempts: 5,
    };

    let processed = process_available(&delivery.pool, &client, &config).await;
    let empty = process_available(&delivery.pool, &client, &config).await;
    let snapshot = delivery.snapshot().await;
    let hits = delivery.receiver.hits.lock().unwrap().len();
    delivery.close().await;

    assert_eq!(processed.unwrap(), true);
    assert_eq!(empty.unwrap(), false);
    assert_eq!(snapshot.status, "delivered");
    assert_eq!(hits, 1);
}

#[tokio::test]
async fn failed_delivery_marks_the_event_failed() {
    let _guard = db_lock().lock().await;
    let delivery = Delivery::open(
        vec![StatusCode::INTERNAL_SERVER_ERROR],
        "test-secret",
        json!({}),
    )
    .await;

    let result = deliver_once(&delivery.pool, delivery.event_id, 1).await;
    let snapshot = delivery.snapshot().await;
    let hits = delivery.receiver.hits.lock().unwrap().len();
    delivery.close().await;

    result.unwrap();
    assert_eq!(snapshot.status, "failed");
    assert_eq!(snapshot.attempts, vec![(1, false)]);
    assert_eq!(hits, 1);
}

#[tokio::test]
async fn temporary_failure_is_retried_and_then_delivered() {
    let _guard = db_lock().lock().await;
    let delivery = Delivery::open(
        vec![StatusCode::INTERNAL_SERVER_ERROR, StatusCode::OK],
        "test-secret",
        json!({}),
    )
    .await;

    let first = deliver_once(&delivery.pool, delivery.event_id, 5).await;
    let after_failure = delivery.snapshot().await;
    delivery.make_due().await;
    let second = deliver_once(&delivery.pool, delivery.event_id, 5).await;
    let after_retry = delivery.snapshot().await;
    let hits = delivery.receiver.hits.lock().unwrap().len();
    delivery.close().await;

    first.unwrap();
    second.unwrap();
    assert_eq!(after_failure.status, "pending");
    assert!(after_failure.next_attempt_at > chrono::Utc::now() - chrono::Duration::seconds(1));
    assert_eq!(after_retry.status, "delivered");
    assert_eq!(after_retry.attempts, vec![(1, false), (2, true)]);
    assert_eq!(hits, 2);
}

#[tokio::test]
async fn delivery_fails_when_attempts_are_exhausted() {
    let _guard = db_lock().lock().await;
    let delivery = Delivery::open(
        vec![StatusCode::INTERNAL_SERVER_ERROR],
        "test-secret",
        json!({}),
    )
    .await;

    let first = deliver_once(&delivery.pool, delivery.event_id, 2).await;
    delivery.make_due().await;
    let second = deliver_once(&delivery.pool, delivery.event_id, 2).await;
    let snapshot = delivery.snapshot().await;
    let another = claim_event(&delivery.pool, delivery.event_id, Duration::from_secs(30)).await;
    let hits = delivery.receiver.hits.lock().unwrap().len();
    delivery.close().await;

    first.unwrap();
    second.unwrap();
    assert_eq!(snapshot.status, "failed");
    assert_eq!(snapshot.attempt_count, 2);
    assert!(another.unwrap().is_none());
    assert_eq!(hits, 2);
}

#[test]
fn backoff_grows_and_then_stops() {
    assert_eq!(backoff(1), Duration::from_secs(1));
    assert_eq!(backoff(2), Duration::from_secs(2));
    assert_eq!(backoff(3), Duration::from_secs(4));
    assert_eq!(backoff(20), Duration::from_secs(300));
}

#[test]
fn decide_retries_until_the_last_attempt() {
    assert_eq!(decide(true, 1, 5).0, EventStatus::Delivered);
    assert_eq!(decide(false, 1, 5).0, EventStatus::Pending);
    assert_eq!(decide(false, 1, 5).1, Some(1.0));
    assert_eq!(decide(false, 5, 5).0, EventStatus::Failed);
    assert_eq!(decide(false, 5, 5).1, None);
}
