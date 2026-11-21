//! Logical value and schema model shared by every source and the sink.
//!
//! The model is deliberately small: six types, a symmetric promotion lattice
//! and one canonical hash per schema. Sources convert their own representation
//! (CSV text, JSON, Elasticsearch `_source`) into [`Value`]; the engine owns all
//! coercion and promotion decisions so there is exactly one place to reason
//! about type changes and one place to test them.

use std::fmt;
use std::sync::Arc;

use arrow_schema::{DataType, Field as ArrowField, Schema as ArrowSchema, TimeUnit};
use serde::{Deserialize, Serialize};
use xxhash_rust::xxh3::xxh3_64;

/// Physical type of a column. `Null` only exists while inferring; a persisted
/// column is always one of the concrete types.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Ty {
    Null,
    Bool,
    I64,
    F64,
    Str,
    TimestampMillis,
}

impl Ty {
    pub const fn as_str(self) -> &'static str {
        match self {
            Ty::Null => "null",
            Ty::Bool => "bool",
            Ty::I64 => "i64",
            Ty::F64 => "f64",
            Ty::Str => "str",
            Ty::TimestampMillis => "timestamp_ms",
        }
    }

    pub fn parse(s: &str) -> Option<Ty> {
        Some(match s {
            "null" => Ty::Null,
            "bool" => Ty::Bool,
            "i64" => Ty::I64,
            "f64" => Ty::F64,
            "str" => Ty::Str,
            "timestamp_ms" => Ty::TimestampMillis,
            _ => return None,
        })
    }

    pub fn arrow_type(self) -> DataType {
        match self {
            Ty::Null | Ty::Str => DataType::Utf8,
            Ty::Bool => DataType::Boolean,
            Ty::I64 => DataType::Int64,
            Ty::F64 => DataType::Float64,
            Ty::TimestampMillis => DataType::Timestamp(TimeUnit::Millisecond, None),
        }
    }
}

impl fmt::Display for Ty {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// One cell. Strings own their bytes: inference and the sink need values that
/// outlive the JSON document or CSV record they came from.
#[derive(Clone, Debug, PartialEq)]
pub enum Value {
    Null,
    Bool(bool),
    I64(i64),
    F64(f64),
    Str(Arc<str>),
    Timestamp(i64),
}

impl Value {
    pub fn ty(&self) -> Ty {
        match self {
            Value::Null => Ty::Null,
            Value::Bool(_) => Ty::Bool,
            Value::I64(_) => Ty::I64,
            Value::F64(_) => Ty::F64,
            Value::Str(_) => Ty::Str,
            Value::Timestamp(_) => Ty::TimestampMillis,
        }
    }

    /// Render for JSON output (manifest previews, `inspect --json`).
    pub fn to_json(&self) -> serde_json::Value {
        match self {
            Value::Null => serde_json::Value::Null,
            Value::Bool(b) => serde_json::Value::Bool(*b),
            Value::I64(i) => serde_json::Value::from(*i),
            Value::F64(f) => serde_json::Number::from_f64(*f)
                .map(serde_json::Value::Number)
                .unwrap_or(serde_json::Value::Null),
            Value::Str(s) => serde_json::Value::String(s.to_string()),
            Value::Timestamp(ms) => serde_json::Value::String(render_timestamp(*ms)),
        }
    }

    /// Human form used in TUI previews and log lines.
    pub fn display(&self) -> String {
        match self {
            Value::Null => String::new(),
            Value::Bool(b) => b.to_string(),
            Value::I64(i) => i.to_string(),
            Value::F64(f) => f.to_string(),
            Value::Str(s) => s.to_string(),
            Value::Timestamp(ms) => render_timestamp(*ms),
        }
    }

    /// Escape hatch to text for `Str`-typed columns. Every type is
    /// stringifiable, so widening `X -> Str` never loses a value.
    pub fn into_string(self) -> String {
        match self {
            Value::Null => String::new(),
            Value::Str(s) => s.to_string(),
            other => other.display(),
        }
    }
}

pub fn render_timestamp(ms: i64) -> String {
    match chrono::DateTime::from_timestamp_millis(ms) {
        Some(dt) => dt.format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string(),
        None => ms.to_string(),
    }
}

/// Parse the timestamp shapes that show up in the wild: RFC 3339 (Z or offset)
/// and Elasticsearch's default `yyyy-MM-dd HH:mm:ss[.SSS]`.
pub fn parse_timestamp(s: &str) -> Option<i64> {
    if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(s) {
        return Some(dt.timestamp_millis());
    }
    for fmt in ["%Y-%m-%d %H:%M:%S%.f", "%Y-%m-%dT%H:%M:%S%.f"] {
        if let Ok(dt) = chrono::NaiveDateTime::parse_from_str(s, fmt) {
            return Some(dt.and_utc().timestamp_millis());
        }
    }
    None
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Field {
    pub name: String,
    pub ty: Ty,
}

impl Field {
    pub fn new(name: impl Into<String>, ty: Ty) -> Self {
        Field {
            name: name.into(),
            ty,
        }
    }
}

/// Ordered, append-only schema. Order is the column order written to Parquet
/// and is part of the hash, so two runs with the same inputs produce the same
/// bytes and the same manifest entry.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct Schema {
    pub fields: Vec<Field>,
}

impl Schema {
    pub fn new(fields: Vec<Field>) -> Self {
        Schema { fields }
    }

    pub fn field(&self, name: &str) -> Option<(usize, &Field)> {
        self.fields.iter().enumerate().find(|(_, f)| f.name == name)
    }

    pub fn index_of(&self, name: &str) -> Option<usize> {
        self.fields.iter().position(|f| f.name == name)
    }

    /// Append a field if the name is new. Returns `true` when the schema grew.
    pub fn insert(&mut self, name: &str, ty: Ty) -> bool {
        if self.index_of(name).is_some() {
            return false;
        }
        self.fields.push(Field::new(name, ty));
        true
    }

    pub fn hash(&self) -> String {
        let mut canon = String::with_capacity(self.fields.len() * 24);
        for f in &self.fields {
            canon.push_str(&f.name);
            canon.push('\u{1f}');
            canon.push_str(f.ty.as_str());
            canon.push('\n');
        }
        format!("{:016x}", xxh3_64(canon.as_bytes()))
    }

    pub fn to_arrow(&self) -> ArrowSchema {
        let fields: Vec<ArrowField> = self
            .fields
            .iter()
            .map(|f| ArrowField::new(f.name.clone(), f.ty.arrow_type(), true))
            .collect();
        ArrowSchema::new(fields)
    }

    /// Names in column order; handy for the engine's name -> index map.
    pub fn names(&self) -> Vec<String> {
        self.fields.iter().map(|f| f.name.clone()).collect()
    }
}

/// Symmetric promotion lattice. There is no "demotion": once a column is `Str`
/// it stays `Str`, which keeps already-written files valid.
///
/// `Bool` never promotes to a number (true is not 1 in a lakehouse) and
/// timestamps only merge with integers (epoch millis) or wider text.
pub fn merge_ty(a: Ty, b: Ty) -> Ty {
    use Ty::*;
    if a == b {
        return a;
    }
    match (a, b) {
        (Null, other) | (other, Null) => other,
        (I64, F64) | (F64, I64) => F64,
        (TimestampMillis, I64) | (I64, TimestampMillis) => TimestampMillis,
        _ => Str,
    }
}

/// Coerce one value to the column type. `None` means "not representable",
/// which the engine turns into a promotion (or an error in strict mode).
pub fn coerce(value: Value, ty: Ty) -> Option<Value> {
    use Value::*;
    if matches!(value, Null) {
        return Some(Null);
    }
    if value.ty() == ty {
        return Some(value);
    }
    Some(match (value, ty) {
        (v, Ty::Str) => Str(Arc::from(v.into_string())),
        (Str(s), Ty::I64) => I64(s.trim().parse::<i64>().ok()?),
        (Str(s), Ty::F64) => F64(s.trim().parse::<f64>().ok()?),
        (Str(s), Ty::Bool) => match s.trim() {
            "true" | "TRUE" | "True" | "1" => Bool(true),
            "false" | "FALSE" | "False" | "0" => Bool(false),
            _ => return None,
        },
        (Str(s), Ty::TimestampMillis) => Timestamp(parse_timestamp(&s)?),
        (I64(i), Ty::F64) => F64(i as f64),
        (F64(f), Ty::I64) => {
            if f.fract() == 0.0 && f >= i64::MIN as f64 && f <= i64::MAX as f64 {
                I64(f as i64)
            } else {
                return None;
            }
        }
        (I64(i), Ty::TimestampMillis) => Timestamp(i),
        (Bool(b), Ty::I64) => I64(if b { 1 } else { 0 }),
        (Bool(b), Ty::F64) => F64(if b { 1.0 } else { 0.0 }),
        _ => return None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lattice_is_symmetric_and_never_loses_text() {
        use Ty::*;
        for a in [Null, Bool, I64, F64, Str, TimestampMillis] {
            for b in [Null, Bool, I64, F64, Str, TimestampMillis] {
                assert_eq!(merge_ty(a, b), merge_ty(b, a), "{a} vs {b}");
                assert_eq!(merge_ty(a, a), a);
            }
        }
        assert_eq!(merge_ty(I64, F64), F64);
        assert_eq!(merge_ty(Bool, I64), Str);
        assert_eq!(merge_ty(TimestampMillis, I64), TimestampMillis);
    }

    #[test]
    fn coercion_accepts_values_that_fit() {
        assert_eq!(
            coerce(Value::Str("42".into()), Ty::I64),
            Some(Value::I64(42))
        );
        assert_eq!(coerce(Value::I64(42), Ty::F64), Some(Value::F64(42.0)));
        assert_eq!(
            coerce(Value::Str("true".into()), Ty::Bool),
            Some(Value::Bool(true))
        );
        assert_eq!(
            coerce(
                Value::Str("2026-11-21T00:00:00Z".into()),
                Ty::TimestampMillis
            ),
            Some(Value::Timestamp(1_795_219_200_000))
        );
        assert_eq!(coerce(Value::F64(1.5), Ty::I64), None);
        assert_eq!(coerce(Value::Str("abc".into()), Ty::I64), None);
        assert_eq!(coerce(Value::Null, Ty::I64), Some(Value::Null));
    }

    #[test]
    fn schema_hash_is_order_sensitive_and_type_sensitive() {
        let a = Schema::new(vec![Field::new("x", Ty::I64), Field::new("y", Ty::Str)]);
        let b = Schema::new(vec![Field::new("y", Ty::Str), Field::new("x", Ty::I64)]);
        let c = Schema::new(vec![Field::new("x", Ty::I64), Field::new("y", Ty::F64)]);
        assert_ne!(a.hash(), b.hash());
        assert_ne!(a.hash(), c.hash());
        assert_eq!(a.hash(), a.clone().hash());
    }

    #[test]
    fn timestamp_parsing_covers_rfc3339_and_es_default() {
        assert_eq!(
            parse_timestamp("2026-11-21T00:00:00.000Z"),
            parse_timestamp("2026-11-21 00:00:00.000")
        );
        assert!(parse_timestamp("not a time").is_none());
    }
}
