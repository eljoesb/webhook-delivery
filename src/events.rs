use axum::Json;
use axum::extract::rejection::{JsonRejection, PathRejection};
use axum::extract::{Path, State};
use axum::http::StatusCode;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::endpoints::AppState;
use crate::error::ApiError;

const MAX_EVENT_TYPE_LEN: usize = 200;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum EventStatus {
    Pending,
    InProgress,
    Delivered,
    Failed,
}

impl EventStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::InProgress => "in_progress",
            Self::Delivered => "delivered",
            Self::Failed => "failed",
        }
    }

    pub fn from_db(value: &str) -> Result<Self, ApiError> {
        match value {
            "pending" => Ok(Self::Pending),
            "in_progress" => Ok(Self::InProgress),
            "delivered" => Ok(Self::Delivered),
            "failed" => Ok(Self::Failed),
            _ => Err(ApiError::Database(sqlx::Error::Protocol(format!(
                "unexpected event status: {value}"
            )))),
        }
    }
}

#[derive(Deserialize)]
pub struct CreateEvent {
    #[serde(rename = "type")]
    event_type: String,
    payload: serde_json::Value,
}

#[derive(Serialize)]
pub struct EventAccepted {
    id: Uuid,
    status: EventStatus,
}

#[derive(Serialize)]
pub struct EventView {
    id: Uuid,
    endpoint_id: Uuid,
    #[serde(rename = "type")]
    event_type: String,
    payload: serde_json::Value,
    status: EventStatus,
    attempt_count: i32,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
}

#[derive(sqlx::FromRow)]
struct EventRow {
    id: Uuid,
    endpoint_id: Uuid,
    event_type: String,
    payload: serde_json::Value,
    status: String,
    attempt_count: i32,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
}

impl EventRow {
    fn into_view(self) -> Result<EventView, ApiError> {
        Ok(EventView {
            id: self.id,
            endpoint_id: self.endpoint_id,
            event_type: self.event_type,
            payload: self.payload,
            status: EventStatus::from_db(&self.status)?,
            attempt_count: self.attempt_count,
            created_at: self.created_at,
            updated_at: self.updated_at,
        })
    }
}

#[derive(Serialize, sqlx::FromRow)]
pub struct AttemptView {
    id: Uuid,
    attempt_number: i32,
    http_status: Option<i32>,
    response_body: Option<String>,
    error_message: Option<String>,
    succeeded: bool,
    created_at: DateTime<Utc>,
}

pub fn validate_event_type(event_type: &str) -> Result<String, ApiError> {
    let event_type = event_type.trim();
    if event_type.is_empty() {
        return Err(ApiError::InvalidInput("type is required".into()));
    }
    if event_type.chars().count() > MAX_EVENT_TYPE_LEN {
        return Err(ApiError::InvalidInput(format!(
            "type must be at most {MAX_EVENT_TYPE_LEN} characters"
        )));
    }
    Ok(event_type.to_owned())
}

pub async fn create_event(
    State(state): State<AppState>,
    path: Result<Path<Uuid>, PathRejection>,
    payload: Result<Json<CreateEvent>, JsonRejection>,
) -> Result<(StatusCode, Json<EventAccepted>), ApiError> {
    let Path(endpoint_id) = path?;
    let Json(payload) = payload?;
    let event_type = validate_event_type(&payload.event_type)?;

    let exists = endpoint_exists(&state.pool, endpoint_id)
        .await
        .map_err(ApiError::Database)?;
    if !exists {
        return Err(ApiError::NotFound(format!(
            "endpoint {endpoint_id} not found"
        )));
    }

    let id = Uuid::new_v4();
    insert_event(&state.pool, id, endpoint_id, &event_type, &payload.payload)
        .await
        .map_err(ApiError::Database)?;

    tracing::info!(%id, %endpoint_id, %event_type, "event accepted");
    Ok((
        StatusCode::ACCEPTED,
        Json(EventAccepted {
            id,
            status: EventStatus::Pending,
        }),
    ))
}

pub async fn get_event(
    State(state): State<AppState>,
    path: Result<Path<Uuid>, PathRejection>,
) -> Result<Json<EventView>, ApiError> {
    let Path(id) = path?;
    let event = find_event(&state.pool, id)
        .await
        .map_err(ApiError::Database)?
        .ok_or_else(|| ApiError::NotFound(format!("event {id} not found")))?;
    Ok(Json(event.into_view()?))
}

pub async fn list_attempts(
    State(state): State<AppState>,
    path: Result<Path<Uuid>, PathRejection>,
) -> Result<Json<Vec<AttemptView>>, ApiError> {
    let Path(id) = path?;
    let exists = event_exists(&state.pool, id)
        .await
        .map_err(ApiError::Database)?;
    if !exists {
        return Err(ApiError::NotFound(format!("event {id} not found")));
    }

    let attempts = sqlx::query_as(
        r#"
        SELECT id, attempt_number, http_status, response_body, error_message, succeeded, created_at
        FROM delivery_attempts
        WHERE event_id = $1
        ORDER BY attempt_number
        "#,
    )
    .bind(id)
    .fetch_all(&state.pool)
    .await
    .map_err(ApiError::Database)?;

    Ok(Json(attempts))
}

async fn event_exists(pool: &sqlx::PgPool, id: Uuid) -> Result<bool, sqlx::Error> {
    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM events WHERE id = $1)")
        .bind(id)
        .fetch_one(pool)
        .await
}

async fn endpoint_exists(pool: &sqlx::PgPool, id: Uuid) -> Result<bool, sqlx::Error> {
    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM endpoints WHERE id = $1)")
        .bind(id)
        .fetch_one(pool)
        .await
}

async fn insert_event(
    pool: &sqlx::PgPool,
    id: Uuid,
    endpoint_id: Uuid,
    event_type: &str,
    payload: &serde_json::Value,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"
        INSERT INTO events (id, endpoint_id, event_type, payload)
        VALUES ($1, $2, $3, $4)
        "#,
    )
    .bind(id)
    .bind(endpoint_id)
    .bind(event_type)
    .bind(payload)
    .execute(pool)
    .await?;
    Ok(())
}

async fn find_event(pool: &sqlx::PgPool, id: Uuid) -> Result<Option<EventRow>, sqlx::Error> {
    sqlx::query_as(
        r#"
        SELECT id, endpoint_id, event_type, payload, status, attempt_count, created_at, updated_at
        FROM events
        WHERE id = $1
        "#,
    )
    .bind(id)
    .fetch_optional(pool)
    .await
}
