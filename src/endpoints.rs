use axum::Json;
use axum::extract::rejection::PathRejection;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::error::ApiError;

const MAX_NAME_LEN: usize = 200;

#[derive(Clone)]
pub struct AppState {
    pub(crate) pool: sqlx::PgPool,
}

impl AppState {
    pub fn new(pool: sqlx::PgPool) -> Self {
        Self { pool }
    }
}

#[derive(Deserialize)]
pub struct CreateEndpoint {
    name: String,
    url: String,
}

#[derive(Serialize)]
pub struct EndpointCreated {
    id: Uuid,
    name: String,
    url: String,
    secret: String,
    created_at: DateTime<Utc>,
}

#[derive(Serialize, sqlx::FromRow)]
pub struct EndpointView {
    id: Uuid,
    name: String,
    url: String,
    created_at: DateTime<Utc>,
}

pub fn validate_name(name: &str) -> Result<String, ApiError> {
    let name = name.trim();
    if name.is_empty() {
        return Err(ApiError::InvalidInput("name is required".into()));
    }
    if name.chars().count() > MAX_NAME_LEN {
        return Err(ApiError::InvalidInput(format!(
            "name must be at most {MAX_NAME_LEN} characters"
        )));
    }
    Ok(name.to_owned())
}

pub fn validate_url(url: &str) -> Result<(), ApiError> {
    let parsed =
        reqwest::Url::parse(url).map_err(|_| ApiError::InvalidInput("url is not valid".into()))?;
    if !matches!(parsed.scheme(), "http" | "https") || parsed.host_str().is_none() {
        return Err(ApiError::InvalidInput(
            "url must be an absolute http or https url".into(),
        ));
    }
    Ok(())
}

pub fn new_secret() -> String {
    let mut bytes = [0u8; 32];
    rand::fill(&mut bytes);
    hex::encode(bytes)
}

pub async fn create_endpoint(
    State(state): State<AppState>,
    payload: Result<Json<CreateEndpoint>, axum::extract::rejection::JsonRejection>,
) -> Result<(StatusCode, Json<EndpointCreated>), ApiError> {
    let Json(payload) = payload?;
    let name = validate_name(&payload.name)?;
    validate_url(&payload.url)?;

    let id = Uuid::new_v4();
    let secret = new_secret();
    let created_at = insert_endpoint(&state.pool, id, &name, &payload.url, &secret)
        .await
        .map_err(ApiError::Database)?;

    tracing::info!(%id, %name, "endpoint created");
    Ok((
        StatusCode::CREATED,
        Json(EndpointCreated {
            id,
            name,
            url: payload.url,
            secret,
            created_at,
        }),
    ))
}

pub async fn list_endpoints(
    State(state): State<AppState>,
) -> Result<Json<Vec<EndpointView>>, ApiError> {
    let endpoints = sqlx::query_as(
        r#"
        SELECT id, name, url, created_at
        FROM endpoints
        ORDER BY created_at
        "#,
    )
    .fetch_all(&state.pool)
    .await
    .map_err(ApiError::Database)?;
    Ok(Json(endpoints))
}

pub async fn get_endpoint(
    State(state): State<AppState>,
    path: Result<Path<Uuid>, PathRejection>,
) -> Result<Json<EndpointView>, ApiError> {
    let Path(id) = path?;
    let endpoint = sqlx::query_as(
        r#"
        SELECT id, name, url, created_at
        FROM endpoints
        WHERE id = $1
        "#,
    )
    .bind(id)
    .fetch_optional(&state.pool)
    .await
    .map_err(ApiError::Database)?
    .ok_or_else(|| ApiError::NotFound(format!("endpoint {id} not found")))?;
    Ok(Json(endpoint))
}

async fn insert_endpoint(
    pool: &sqlx::PgPool,
    id: Uuid,
    name: &str,
    url: &str,
    secret: &str,
) -> Result<DateTime<Utc>, sqlx::Error> {
    sqlx::query_scalar(
        r#"
        INSERT INTO endpoints (id, name, url, secret)
        VALUES ($1, $2, $3, $4)
        RETURNING created_at
        "#,
    )
    .bind(id)
    .bind(name)
    .bind(url)
    .bind(secret)
    .fetch_one(pool)
    .await
}
