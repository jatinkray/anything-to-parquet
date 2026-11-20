//! Elasticsearch `_source` / JSONL documents -> flat named values.
//!
//! Nested objects become dotted column names (`http.request.method`). Arrays
//! are kept losslessly but as JSON text, because a stable Parquet column cannot
//! change from scalar to list mid-file; users who need real list columns can
//! re-read the JSON text. This is a documented, tested trade-off, not an
//! accident.

use serde_json::{Map, Value as Json};

use crate::value::{parse_timestamp, Value};

/// Flatten one document. Object keys are joined with `.`; when `timestamp` is
/// `Some(name)` the matching dotted key is parsed into epoch millis and fails
/// loudly instead of silently becoming text.
pub fn flatten_document(doc: &Json, timestamp: Option<&str>) -> Vec<(String, Value)> {
    let mut out = Vec::new();
    let mut key = String::new();
    match doc {
        Json::Object(map) => flatten_object(map, &mut key, timestamp, &mut out),
        // A JSONL file whose lines are scalars still deserves a column.
        other => out.push(("value".to_string(), scalar(other, "value", timestamp))),
    }
    out
}

fn flatten_object(
    map: &Map<String, Json>,
    prefix: &mut String,
    timestamp: Option<&str>,
    out: &mut Vec<(String, Value)>,
) {
    for (k, v) in map {
        let saved = prefix.len();
        if !prefix.is_empty() {
            prefix.push('.');
        }
        prefix.push_str(k);
        match v {
            Json::Object(inner) => flatten_object(inner, prefix, timestamp, out),
            other => out.push((prefix.clone(), scalar(other, prefix, timestamp))),
        }
        prefix.truncate(saved);
    }
}

/// Convert a JSON scalar to a [`Value`]; arrays/objects that survive flattening
/// are encoded as JSON text.
fn scalar(json: &Json, dotted_key: &str, timestamp: Option<&str>) -> Value {
    match json {
        Json::Null => Value::Null,
        Json::Bool(b) => Value::Bool(*b),
        Json::Number(n) => match n.as_i64() {
            Some(i) => Value::I64(i),
            None => match n.as_f64() {
                Some(f) => Value::F64(f),
                None => Value::Str(n.to_string()),
            },
        },
        Json::String(s) => {
            if timestamp.is_some_and(|t| t == dotted_key) {
                match parse_timestamp(s) {
                    Some(ms) => Value::Timestamp(ms),
                    // Keep the raw text; the engine reports a typed error with
                    // the column and record number when strict mode is on.
                    None => Value::Str(s.clone()),
                }
            } else {
                Value::Str(s.clone())
            }
        }
        Json::Array(_) | Json::Object(_) => Value::Str(json.to_string()),
    }
}

/// Flatten `_source` while forcing the timestamp column to be parsed. Returns
/// the values plus the raw text of the timestamp field when it did not parse,
/// so the caller can raise `BadTimestamp` with a useful message.
pub fn flatten_with_timestamp(
    doc: &Json,
    timestamp: Option<&str>,
) -> (Vec<(String, Value)>, Option<String>) {
    let values = flatten_document(doc, timestamp);
    let bad = timestamp.and_then(|name| {
        values.iter().find_map(|(k, v)| {
            if k == name {
                match v {
                    Value::Str(s) => Some(s.clone()),
                    Value::Null => None,
                    _ => None,
                }
            } else {
                None
            }
        })
    });
    (values, bad)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn map_of(doc: Json, ts: Option<&str>) -> std::collections::BTreeMap<String, Value> {
        flatten_document(&doc, ts).into_iter().collect()
    }

    #[test]
    fn nested_objects_become_dotted_columns() {
        let doc = json!({
            "http": {"request": {"method": "GET", "bytes": 512}},
            "message": "ok",
            "count": 3,
            "ratio": 0.5,
            "ok": true
        });
        let m = map_of(doc, None);
        assert_eq!(m["http.request.method"], Value::Str("GET".into()));
        assert_eq!(m["http.request.bytes"], Value::I64(512));
        assert_eq!(m["count"], Value::I64(3));
        assert_eq!(m["ratio"], Value::F64(0.5));
        assert_eq!(m["ok"], Value::Bool(true));
    }

    #[test]
    fn arrays_stay_lossless_as_json_text() {
        let doc = json!({"tags": ["a", "b"], "obj": {"k": [1, 2]}});
        let m = map_of(doc, None);
        assert_eq!(m["tags"], Value::Str("[\"a\",\"b\"]".into()));
        assert_eq!(m["obj.k"], Value::Str("[1,2]".into()));
    }

    #[test]
    fn timestamp_column_parses_and_reports_bad_values() {
        let doc = json!({"@timestamp": "2026-11-21T10:00:00Z", "n": 1});
        let m = map_of(doc, Some("@timestamp"));
        assert_eq!(m["@timestamp"], Value::Timestamp(1_795_255_200_000));

        let bad = json!({"@timestamp": "yesterday"});
        let (_, bad_text) = flatten_with_timestamp(&bad, Some("@timestamp"));
        assert_eq!(bad_text.as_deref(), Some("yesterday"));
    }

    #[test]
    fn null_cells_are_preserved_not_dropped() {
        let doc = json!({"a": null, "b": 1});
        let m = map_of(doc, None);
        assert_eq!(m["a"], Value::Null);
        assert_eq!(m["b"], Value::I64(1));
    }
}
