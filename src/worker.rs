use std::time::Duration;

use uuid::Uuid;

use crate::events::EventStatus;
use crate::signature;

pub const MAX_RESPONSE_CHARS: usize = 1024;

#[derive(Debug, Clone, Copy)]
pub struct WorkerConfig {
    pub lease: Duration,
    pub poll_interval: Duration,
    pub request_timeout: Duration,
    pub max_attempts: i32,
}

impl WorkerConfig {
    pub fn from_env() -> Self {
        Self {
            lease: Duration::from_secs(env_u64("LEASE_SECONDS", 30)),
            poll_interval: Duration::from_millis(env_u64("POLL_INTERVAL_MS", 1000)),
            request_timeout: Duration::from_secs(env_u64("HTTP_TIMEOUT_SECONDS", 10)),
            max_attempts: env_u64("MAX_ATTEMPTS", 5) as i32,
        }
    }
}

pub fn spawn(pool: sqlx::PgPool, client: reqwest::Client, config: WorkerConfig) {
    tokio::spawn(async move {
        loop {
            match process_available(&pool, &client, &config).await {
                Ok(true) => {}
                Ok(false) => tokio::time::sleep(config.poll_interval).await,
                Err(error) => {
                    tracing::error!(%error, "worker failed to process an event");
                    tokio::time::sleep(config.poll_interval).await;
                }
            }
        }
    });
}

pub async fn process_available(
    pool: &sqlx::PgPool,
    client: &reqwest::Client,
    config: &WorkerConfig,
) -> Result<bool, sqlx::Error> {
    let Some(event) = claim_due(pool, config.lease).await? else {
        return Ok(false);
    };
    deliver(pool, client, config, event).await?;
    Ok(true)
}

#[derive(Debug, sqlx::FromRow)]
pub struct ClaimedEvent {
    id: Uuid,
    endpoint_id: Uuid,
    event_type: String,
    payload: serde_json::Value,
    attempt_count: i32,
}

#[derive(sqlx::FromRow)]
struct Destination {
    url: String,
    secret: String,
}

struct Outcome {
    http_status: Option<i32>,
    response_body: Option<String>,
    error_message: Option<String>,
    succeeded: bool,
}

async fn claim_due(
    pool: &sqlx::PgPool,
    lease: Duration,
) -> Result<Option<ClaimedEvent>, sqlx::Error> {
    sqlx::query_as(
        r#"
        WITH next_event AS (
            SELECT id
            FROM events
            WHERE next_attempt_at <= now()
              AND (
                  status = 'pending'
                  OR (status = 'in_progress' AND locked_until < now())
              )
            ORDER BY next_attempt_at
            FOR UPDATE SKIP LOCKED
            LIMIT 1
        )
        UPDATE events AS e
        SET status = 'in_progress',
            locked_until = now() + make_interval(secs => $1::double precision),
            updated_at = now()
        FROM next_event
        WHERE e.id = next_event.id
        RETURNING e.id, e.endpoint_id, e.event_type, e.payload, e.attempt_count
        "#,
    )
    .bind(lease.as_secs_f64())
    .fetch_optional(pool)
    .await
}

pub async fn deliver(
    pool: &sqlx::PgPool,
    client: &reqwest::Client,
    config: &WorkerConfig,
    event: ClaimedEvent,
) -> Result<(), sqlx::Error> {
    let attempt_number = event.attempt_count + 1;
    let destination = find_destination(pool, event.endpoint_id).await?;
    let outcome = match destination {
        Some(destination) => post_event(client, &destination, &event, attempt_number).await,
        None => Outcome {
            http_status: None,
            response_body: None,
            error_message: Some("endpoint not found".to_owned()),
            succeeded: false,
        },
    };

    let (status, retry_after_seconds) =
        decide(outcome.succeeded, attempt_number, config.max_attempts);
    record_attempt(
        pool,
        event.id,
        attempt_number,
        status,
        retry_after_seconds,
        &outcome,
    )
    .await?;

    match status {
        EventStatus::Delivered => tracing::info!(
            event_id = %event.id,
            attempt = attempt_number,
            http_status = outcome.http_status,
            "event delivered"
        ),
        EventStatus::Pending => tracing::warn!(
            event_id = %event.id,
            attempt = attempt_number,
            http_status = outcome.http_status,
            error = outcome.error_message.as_deref().unwrap_or(""),
            "delivery failed; retry scheduled"
        ),
        EventStatus::Failed => tracing::warn!(
            event_id = %event.id,
            attempt = attempt_number,
            http_status = outcome.http_status,
            error = outcome.error_message.as_deref().unwrap_or(""),
            "event delivery failed"
        ),
        EventStatus::InProgress => {}
    }
    Ok(())
}

async fn find_destination(
    pool: &sqlx::PgPool,
    endpoint_id: Uuid,
) -> Result<Option<Destination>, sqlx::Error> {
    sqlx::query_as("SELECT url, secret FROM endpoints WHERE id = $1")
        .bind(endpoint_id)
        .fetch_optional(pool)
        .await
}

async fn post_event(
    client: &reqwest::Client,
    destination: &Destination,
    event: &ClaimedEvent,
    attempt_number: i32,
) -> Outcome {
    let body = match serde_json::to_vec(&event.payload) {
        Ok(body) => body,
        Err(error) => {
            return Outcome {
                http_status: None,
                response_body: None,
                error_message: Some(truncate(&error.to_string())),
                succeeded: false,
            };
        }
    };
    let signature = signature::sign(&destination.secret, &body);

    match client
        .post(&destination.url)
        .header("content-type", "application/json")
        .header("x-webhook-signature", signature)
        .header("x-webhook-id", event.id.to_string())
        .header("x-webhook-event", &event.event_type)
        .header("x-webhook-attempt", attempt_number.to_string())
        .body(body)
        .send()
        .await
    {
        Ok(response) => {
            let status = response.status();
            let http_status = i32::from(status.as_u16());
            let response_body = match response.text().await {
                Ok(text) => Some(truncate(&text)),
                Err(error) => Some(truncate(&error.to_string())),
            };
            Outcome {
                http_status: Some(http_status),
                response_body,
                error_message: None,
                succeeded: status.is_success(),
            }
        }
        Err(error) => Outcome {
            http_status: None,
            response_body: None,
            error_message: Some(truncate(&error.to_string())),
            succeeded: false,
        },
    }
}

async fn record_attempt(
    pool: &sqlx::PgPool,
    event_id: Uuid,
    attempt_number: i32,
    status: EventStatus,
    retry_after_seconds: Option<f64>,
    outcome: &Outcome,
) -> Result<(), sqlx::Error> {
    let mut tx = pool.begin().await?;
    sqlx::query(
        r#"
        INSERT INTO delivery_attempts
            (id, event_id, attempt_number, http_status, response_body, error_message, succeeded)
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        "#,
    )
    .bind(Uuid::new_v4())
    .bind(event_id)
    .bind(attempt_number)
    .bind(outcome.http_status)
    .bind(&outcome.response_body)
    .bind(&outcome.error_message)
    .bind(outcome.succeeded)
    .execute(&mut *tx)
    .await?;

    sqlx::query(
        r#"
        UPDATE events
        SET status = $2,
            attempt_count = attempt_count + 1,
            next_attempt_at = COALESCE(
                now() + make_interval(secs => $3::double precision),
                next_attempt_at
            ),
            locked_until = NULL,
            updated_at = now()
        WHERE id = $1
          AND status = 'in_progress'
        "#,
    )
    .bind(event_id)
    .bind(status.as_str())
    .bind(retry_after_seconds)
    .execute(&mut *tx)
    .await?;

    tx.commit().await
}

pub fn truncate(text: &str) -> String {
    if text.chars().count() <= MAX_RESPONSE_CHARS {
        text.to_owned()
    } else {
        text.chars().take(MAX_RESPONSE_CHARS).collect()
    }
}

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

pub fn backoff(attempt_number: u32) -> Duration {
    let shift = attempt_number.saturating_sub(1).min(9);
    Duration::from_secs((1_u64 << shift).min(300))
}

pub fn decide(
    succeeded: bool,
    attempt_number: i32,
    max_attempts: i32,
) -> (EventStatus, Option<f64>) {
    if succeeded {
        (EventStatus::Delivered, None)
    } else if attempt_number >= max_attempts {
        (EventStatus::Failed, None)
    } else {
        (
            EventStatus::Pending,
            Some(backoff(attempt_number as u32).as_secs_f64()),
        )
    }
}

pub async fn claim_event(
    pool: &sqlx::PgPool,
    event_id: Uuid,
    lease: Duration,
) -> Result<Option<ClaimedEvent>, sqlx::Error> {
    sqlx::query_as(
        r#"
        UPDATE events
        SET status = 'in_progress',
            locked_until = now() + make_interval(secs => $1::double precision),
            updated_at = now()
        WHERE id = $2
          AND (
              status = 'pending'
              OR (status = 'in_progress' AND locked_until < now())
          )
        RETURNING id, endpoint_id, event_type, payload, attempt_count
        "#,
    )
    .bind(lease.as_secs_f64())
    .bind(event_id)
    .fetch_optional(pool)
    .await
}
