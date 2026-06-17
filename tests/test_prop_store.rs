//! Integration tests for PropStore — Sprint 1
//!
//! Tests columnar property storage: multi-label isolation,
//! sparse reads, bulk row insert, and concurrent access.

use lightgraph::storage::prop_store::{PropStore, Value};

fn v_int(i: i64) -> Value {
    Value::Int(i)
}

fn v_str(s: &str) -> Value {
    Value::String(s.to_string())
}

#[test]
fn test_multi_label_property_isolation() {
    let store = PropStore::new();

    // Label 0: User properties
    let r0 = store.insert_row(0, &[
        ("name".into(), v_str("Alice")),
        ("age".into(), v_int(30)),
    ]);

    // Label 1: Company properties
    let r1 = store.insert_row(1, &[
        ("name".into(), v_str("Acme Corp")),
        ("employees".into(), v_int(500)),
    ]);

    // Verify isolation
    let user = store.get_row(0, r0);
    assert!(user.iter().any(|(k, _)| k == "name"));
    assert!(user.iter().any(|(k, _)| k == "age"));
    assert!(!user.iter().any(|(k, _)| k == "employees"));

    let company = store.get_row(1, r1);
    assert!(company.iter().any(|(k, _)| k == "name"));
    assert!(company.iter().any(|(k, _)| k == "employees"));
    assert!(!company.iter().any(|(k, _)| k == "age"));
}

#[test]
fn test_sparse_property_reads() {
    let store = PropStore::new();

    // Insert only some columns at some rows
    store.insert_prop(0, "name", 0, Some(v_str("Alice")));
    store.insert_prop(0, "name", 2, Some(v_str("Charlie")));
    store.insert_prop(0, "email", 0, Some(v_str("alice@example.com")));
    store.insert_prop(0, "email", 1, Some(v_str("bob@example.com")));

    // name column: row 0 set, row 1 unset, row 2 set
    assert_eq!(store.get_prop(0, "name", 0), Some(v_str("Alice")));
    assert_eq!(store.get_prop(0, "name", 1), None);
    assert_eq!(store.get_prop(0, "name", 2), Some(v_str("Charlie")));

    // email column: row 0 and 1 set, row 2 unset
    assert_eq!(store.get_prop(0, "email", 0), Some(v_str("alice@example.com")));
    assert_eq!(store.get_prop(0, "email", 1), Some(v_str("bob@example.com")));
    assert_eq!(store.get_prop(0, "email", 2), None);
}

#[test]
fn test_bulk_data_cycle() {
    let store = PropStore::new();

    // Insert 1000 rows with 5 properties each
    for i in 0..1000u32 {
        store.insert_row(0, &[
            ("id".into(), v_int(i as i64)),
            ("name".into(), v_str(&format!("user_{}", i))),
            ("active".into(), Value::Bool(i % 2 == 0)),
            ("score".into(), Value::Float(ordered_float::OrderedFloat(i as f64 * 0.01))),
            ("tags".into(), Value::List(vec![v_int(1), v_int(2)])),
        ]);
    }

    assert_eq!(store.row_count(0), 1000);

    // Spot-check
    assert_eq!(store.get_prop(0, "id", 500), Some(v_int(500)));
    assert_eq!(store.get_prop(0, "name", 0), Some(v_str("user_0")));
    assert_eq!(store.get_prop(0, "active", 1), Some(Value::Bool(false)));
    assert_eq!(store.get_prop(0, "active", 2), Some(Value::Bool(true)));

    // Overwrite
    store.set_prop(0, "name", 500, Some(v_str("renamed_user")));
    assert_eq!(store.get_prop(0, "name", 500), Some(v_str("renamed_user")));
}
