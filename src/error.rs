use axum::Json;
use axum::extract::rejection::{JsonRejection, PathRejection};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use serde::Serialize;

#[derive(Debug, thiserror::Error)]
pub enum ApiError {
    #[error("{0}")]
    InvalidInput(String),

    #[error("{0}")]
    NotFound(String),

    #[error("Database error")]
    Database(#[source] sqlx::Error),
}

#[derive(Serialize)]
struct ErrorBody {
    error: &'static str,
    message: String,
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (status, error) = match self {
            ApiError::InvalidInput(message) => {
                (StatusCode::BAD_REQUEST, ("invalid_input", message))
            }
            ApiError::NotFound(message) => (StatusCode::NOT_FOUND, ("not_found", message)),
            ApiError::Database(error) => {
                tracing::error!(%error, "database error");
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    (
                        "database_error",
                        "An unexpected database error occurred".to_string(),
                    ),
                )
            }
        };
        let (error, message) = error;
        (status, Json(ErrorBody { error, message })).into_response()
    }
}

impl From<JsonRejection> for ApiError {
    fn from(rejection: JsonRejection) -> Self {
        ApiError::InvalidInput(rejection.to_string())
    }
}

impl From<PathRejection> for ApiError {
    fn from(rejection: PathRejection) -> Self {
        ApiError::InvalidInput(rejection.to_string())
    }
}
