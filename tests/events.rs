use webhook_delivery::events::{
    EventStatus,
    validate_event_type,
};

#[test]
fn event_type_is_trimmed_and_required() {
    assert_eq!(
        validate_event_type("  invoice.paid  ").unwrap(),
        "invoice.paid"
    );
    assert!(validate_event_type("   ").is_err());
}

#[test]
fn status_serializes_in_snake_case() {
    let json = serde_json::to_value(EventStatus::InProgress).unwrap();
    assert_eq!(json, serde_json::json!("in_progress"));
    assert_eq!(EventStatus::Delivered.as_str(), "delivered");
    assert_eq!(EventStatus::Failed.as_str(), "failed");
}

#[test]
fn status_from_db_rejects_unknown_values() {
    assert_eq!(
        EventStatus::from_db("pending").unwrap(),
        EventStatus::Pending
    );
    assert!(EventStatus::from_db("sent").is_err());
}
