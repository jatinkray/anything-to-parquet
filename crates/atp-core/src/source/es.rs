//! Elasticsearch / OpenSearch source.
//!
//! Schema comes from `GET /{index}/_mapping` (the mapping is authoritative:
//! `keyword` stays text even when every sampled value is numeric). Rows come
//! from the scroll API with a per-slice checkpoint, so a 300 GB backfill that
//! dies at hour six resumes instead of starting over.
//!
//! v0.1 walks slices sequentially. Each slice has its own scroll context and
//! checkpoint entry; parallelism is deliberately left out until the resume
//! semantics are proven.

use std::collections::BTreeMap;
use std::path::PathBuf;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value as Json};

use crate::error::{Error, Result};
use crate::flatten::flatten_document;
use crate::value::{merge_ty, Field, Schema, Ty, Value};
use xxhash_rust::xxh3::xxh3_64;

use super::{EsConfig, Page, Source, SourceConfig, SourceInfo};

pub struct EsSource {
    base: String,
    index: String,
    cfg: EsConfig,
    timestamp: Option<String>,
    client: reqwest::Client,
    columns: Vec<String>,
    schema: Schema,
    rows_estimate: Option<u64>,
    bytes_estimate: Option<u64>,
    started: bool,
    first_page: Option<Json>,
    scroll_id: Option<String>,
    slice: usize,
    rows_fetched: u64,
    checkpoint_path: PathBuf,
    query_hash: String,
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct Checkpoint {
    index: String,
    query_hash: String,
    slice: usize,
    slices: usize,
    scroll_id: Option<String>,
    rows_fetched: u64,
}

impl EsSource {
    pub fn new(config: &SourceConfig) -> Result<Self> {
        let base = config.location.trim_end_matches('/').to_string();
        let cfg = config.es.clone();
        if cfg.index.trim().is_empty() {
            return Err(Error::msg("--index is required for Elasticsearch sources"));
        }
        let mut headers = reqwest::header::HeaderMap::new();
        if let Some(key) = cfg.api_key.as_deref() {
            let value = reqwest::header::HeaderValue::from_str(&format!("ApiKey {key}"))
                .map_err(|e| Error::msg(format!("invalid --api-key: {e}")))?;
            headers.insert(reqwest::header::AUTHORIZATION, value);
        }
        let client = reqwest::Client::builder()
            .default_headers(headers)
            .danger_accept_invalid_certs(cfg.insecure)
            .build()?;
        let query_hash = format!(
            "{:016x}",
            xxh3_64(
                format!("{}\u{1f}{}", cfg.index, cfg.query.as_deref().unwrap_or("")).as_bytes()
            )
        );
        let checkpoint_path = cfg
            .checkpoint_dir
            .join(format!("checkpoint-{}.json", sanitize(&cfg.index)));
        Ok(EsSource {
            base,
            index: cfg.index.clone(),
            cfg,
            timestamp: config.timestamp_column.clone(),
            client,
            columns: Vec::new(),
            schema: Schema::default(),
            rows_estimate: None,
            bytes_estimate: None,
            started: false,
            first_page: None,
            scroll_id: None,
            slice: 0,
            rows_fetched: 0,
            checkpoint_path,
            query_hash,
        })
    }

    fn url(&self, path: &str) -> String {
        format!(
            "{}/{}/{}",
            self.base,
            self.index,
            path.trim_start_matches('/')
        )
    }

    async fn get_json(&self, url: String) -> Result<Json> {
        let resp = self.client.get(url).send().await?;
        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(Error::source_err(format!(
                "GET failed with {status}: {}",
                truncate(&body, 400)
            )));
        }
        Ok(resp.json().await?)
    }

    async fn mapping_schema(&mut self) -> Result<()> {
        let body = self.get_json(self.url("_mapping")).await?;
        let mut types: BTreeMap<String, Ty> = BTreeMap::new();
        if let Json::Object(indices) = &body {
            for (_index, desc) in indices {
                if let Some(props) = desc
                    .get("mappings")
                    .and_then(|m| m.get("properties"))
                    .and_then(|p| p.as_object())
                {
                    collect_properties("", props, &mut types);
                }
            }
        }
        if types.is_empty() {
            tracing::warn!(
                index = %self.index,
                "mapping exposed no properties; schema will be discovered from documents"
            );
        }
        if let Some(ts) = &self.timestamp {
            types.insert(ts.clone(), Ty::TimestampMillis);
        }
        self.columns = types.keys().cloned().collect();
        self.schema = Schema::new(
            types
                .into_iter()
                .map(|(name, ty)| Field::new(name, ty))
                .collect(),
        );
        Ok(())
    }

    async fn estimates(&mut self) {
        if let Ok(body) = self.get_json(self.url("_count")).await {
            self.rows_estimate = body.get("count").and_then(|c| c.as_u64());
        }
        if let Ok(body) = self.get_json(self.url("_stats/store")).await {
            let mut total = 0u64;
            if let Some(indices) = body.get("indices").and_then(|i| i.as_object()) {
                for (_name, stats) in indices {
                    total += stats
                        .get("primaries")
                        .and_then(|p| p.get("store"))
                        .and_then(|s| s.get("size_in_bytes"))
                        .and_then(|b| b.as_u64())
                        .unwrap_or(0);
                }
            }
            if total > 0 {
                self.bytes_estimate = Some(total);
            }
        }
    }

    fn load_checkpoint(&self) -> Option<Checkpoint> {
        let text = std::fs::read_to_string(&self.checkpoint_path).ok()?;
        let cp: Checkpoint = serde_json::from_str(&text).ok()?;
        if cp.index == self.index && cp.query_hash == self.query_hash {
            Some(cp)
        } else {
            None
        }
    }

    fn save_checkpoint(&self) -> Result<()> {
        if let Some(dir) = self.checkpoint_path.parent() {
            std::fs::create_dir_all(dir)?;
        }
        let cp = Checkpoint {
            index: self.index.clone(),
            query_hash: self.query_hash.clone(),
            slice: self.slice,
            slices: self.cfg.slices.max(1),
            scroll_id: self.scroll_id.clone(),
            rows_fetched: self.rows_fetched,
        };
        let tmp = self.checkpoint_path.with_extension("json.tmp");
        std::fs::write(&tmp, serde_json::to_vec_pretty(&cp)?)?;
        std::fs::rename(&tmp, &self.checkpoint_path)?;
        Ok(())
    }

    fn clear_checkpoint(&self) {
        let _ = std::fs::remove_file(&self.checkpoint_path);
    }

    async fn start_scroll(&mut self) -> Result<()> {
        let mut body = Map::new();
        body.insert("size".into(), json!(self.cfg.page_size));
        body.insert(
            "query".into(),
            match self.cfg.query.as_deref() {
                Some(q) => serde_json::from_str(q)
                    .map_err(|e| Error::msg(format!("--query is not valid JSON: {e}")))?,
                None => json!({"match_all": {}}),
            },
        );
        body.insert("sort".into(), json!(["_doc"]));
        let slices = self.cfg.slices.max(1);
        if slices > 1 {
            body.insert("slice".into(), json!({"id": self.slice, "max": slices}));
        }
        let url = format!(
            "{}/_search?scroll={}",
            self.url(""),
            urlencode(&self.cfg.scroll)
        );
        let resp = self.client.post(url).json(&body).send().await?;
        let status = resp.status();
        let text = resp.text().await?;
        if !status.is_success() {
            return Err(Error::source_err(format!(
                "search failed with {status}: {}",
                truncate(&text, 400)
            )));
        }
        let doc: Json = serde_json::from_str(&text)?;
        self.scroll_id = doc
            .get("_scroll_id")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        // The initial search already returned a page of hits; hand it to the
        // caller instead of scrolling past it (that would drop the first page).
        self.first_page = Some(doc);
        self.started = true;
        Ok(())
    }

    async fn next_scroll(&mut self) -> Result<Json> {
        let scroll_id = match &self.scroll_id {
            Some(id) => id.clone(),
            None => return Err(Error::source_err("scroll context was never created")),
        };
        let resp = self
            .client
            .post(format!("{}/_search/scroll", self.base))
            .json(&json!({"scroll": self.cfg.scroll, "scroll_id": scroll_id}))
            .send()
            .await?;
        let status = resp.status();
        let text = resp.text().await?;
        if status == reqwest::StatusCode::NOT_FOUND {
            return Err(Error::source_err(format!(
                "scroll context expired for index `{}` (slice {} of {}); rerun without --resume to restart this slice",
                self.index,
                self.slice,
                self.cfg.slices.max(1)
            )));
        }
        if !status.is_success() {
            return Err(Error::source_err(format!(
                "scroll failed with {status}: {}",
                truncate(&text, 400)
            )));
        }
        Ok(serde_json::from_str(&text)?)
    }

    fn collect_hits(&mut self, doc: &Json) -> Vec<Vec<Value>> {
        let hits = doc
            .get("hits")
            .and_then(|h| h.get("hits"))
            .and_then(|h| h.as_array())
            .cloned()
            .unwrap_or_default();
        let mut rows = Vec::with_capacity(hits.len());
        for hit in hits {
            let source = hit.get("_source").cloned().unwrap_or(Json::Null);
            let pairs = flatten_document(&source, self.timestamp.as_deref());
            let mut row = vec![Value::Null; self.columns.len()];
            for (name, value) in pairs {
                let idx = match self.columns.iter().position(|c| c == &name) {
                    Some(i) => i,
                    None => {
                        // Dynamic field not present in the mapping: keep it.
                        self.columns.push(name.clone());
                        self.schema.insert(&name, value.ty());
                        row.push(Value::Null);
                        self.columns.len() - 1
                    }
                };
                row[idx] = value;
            }
            rows.push(row);
        }
        rows
    }
}

#[async_trait]
impl Source for EsSource {
    fn describe(&self) -> SourceInfo {
        let mut notes = vec![format!(
            "index `{}`, scroll {}, page {}, {} slice(s)",
            self.index,
            self.cfg.scroll,
            self.cfg.page_size,
            self.cfg.slices.max(1)
        )];
        if self.cfg.resume {
            notes.push("resume enabled".to_string());
        }
        SourceInfo {
            kind: "elasticsearch".to_string(),
            location: self.base.clone(),
            rows_estimate: self.rows_estimate,
            bytes_estimate: self.bytes_estimate,
            notes,
        }
    }

    async fn infer(&mut self) -> Result<Schema> {
        self.mapping_schema().await?;
        self.estimates().await;
        if self.cfg.resume {
            if let Some(cp) = self.load_checkpoint() {
                self.slice = cp.slice;
                self.rows_fetched = cp.rows_fetched;
                self.scroll_id = cp.scroll_id;
                self.started = self.scroll_id.is_some();
                tracing::info!(
                    slice = self.slice,
                    rows = self.rows_fetched,
                    "resuming from checkpoint"
                );
            }
        }
        Ok(self.schema.clone())
    }

    async fn next_page(&mut self, _max_rows: usize) -> Result<Option<Page>> {
        let slices = self.cfg.slices.max(1);
        loop {
            if !self.started {
                if self.slice >= slices {
                    self.clear_checkpoint();
                    return Ok(None);
                }
                self.start_scroll().await?;
            }
            let doc = match self.first_page.take() {
                Some(doc) => doc,
                None => self.next_scroll().await?,
            };
            if let Some(id) = doc.get("_scroll_id").and_then(|v| v.as_str()) {
                self.scroll_id = Some(id.to_string());
            }
            let rows = self.collect_hits(&doc);
            if rows.is_empty() {
                // Slice exhausted: drop its context and move on.
                self.delete_scroll().await;
                self.slice += 1;
                self.started = false;
                self.first_page = None;
                self.scroll_id = None;
                self.save_checkpoint()?;
                if self.slice >= slices {
                    self.clear_checkpoint();
                    return Ok(None);
                }
                continue;
            }
            self.rows_fetched += rows.len() as u64;
            self.save_checkpoint()?;
            let mut rows = rows;
            for row in rows.iter_mut() {
                row.resize(self.columns.len(), Value::Null);
            }
            return Ok(Some(Page {
                columns: self.columns.clone(),
                rows,
            }));
        }
    }

    fn timestamp_column(&self) -> Option<String> {
        self.timestamp.clone()
    }

    async fn finish(&mut self) -> Result<()> {
        self.delete_scroll().await;
        self.clear_checkpoint();
        Ok(())
    }
}

impl EsSource {
    async fn delete_scroll(&mut self) {
        if let Some(id) = self.scroll_id.take() {
            let _ = self
                .client
                .delete(format!("{}/_search/scroll", self.base))
                .json(&json!({"scroll_id": [id]}))
                .send()
                .await;
        }
    }
}

fn collect_properties(prefix: &str, props: &Map<String, Json>, out: &mut BTreeMap<String, Ty>) {
    for (name, def) in props {
        let dotted = if prefix.is_empty() {
            name.clone()
        } else {
            format!("{prefix}.{name}")
        };
        let ty = def.get("type").and_then(|t| t.as_str()).unwrap_or("object");
        match ty {
            "object" | "nested" => {
                if let Some(inner) = def.get("properties").and_then(|p| p.as_object()) {
                    collect_properties(&dotted, inner, out);
                }
            }
            "long" | "integer" | "short" | "byte" | "unsigned_long" => {
                merge_into(out, &dotted, Ty::I64)
            }
            "float" | "double" | "half_float" | "scaled_float" => merge_into(out, &dotted, Ty::F64),
            "boolean" => merge_into(out, &dotted, Ty::Bool),
            "date" | "date_nanos" => merge_into(out, &dotted, Ty::TimestampMillis),
            _ => merge_into(out, &dotted, Ty::Str),
        }
    }
}

fn merge_into(out: &mut BTreeMap<String, Ty>, name: &str, ty: Ty) {
    match out.get(name).copied() {
        Some(existing) => {
            out.insert(name.to_string(), merge_ty(existing, ty));
        }
        None => {
            out.insert(name.to_string(), ty);
        }
    }
}

fn sanitize(index: &str) -> String {
    index
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect()
}

fn urlencode(s: &str) -> String {
    s.chars()
        .map(|c| match c {
            'a'..='z' | 'A'..='Z' | '0'..='9' | '-' | '_' | '.' | '~' => c.to_string(),
            other => format!("%{:02X}", other as u32),
        })
        .collect()
}

fn truncate(s: &str, n: usize) -> String {
    if s.len() <= n {
        return s.to_string();
    }
    let mut cut = n;
    while cut > 0 && !s.is_char_boundary(cut) {
        cut -= 1;
    }
    format!("{}…", &s[..cut])
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::source::SourceKind;
    use std::sync::{Arc, Mutex};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    /// Minimal scripted Elasticsearch: serves mapping/count/stats and a queue of
    /// scroll responses, recording the paths it saw.
    async fn stub_es(
        scroll_pages: Vec<Json>,
    ) -> (String, Arc<Mutex<Vec<String>>>, tokio::task::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("addr");
        let seen = Arc::new(Mutex::new(Vec::new()));
        let seen_task = seen.clone();
        let handle = tokio::spawn(async move {
            let pages = Arc::new(Mutex::new(scroll_pages));
            loop {
                let (mut sock, _) = match listener.accept().await {
                    Ok(v) => v,
                    Err(_) => break,
                };
                let seen = seen_task.clone();
                let pages = pages.clone();
                tokio::spawn(async move {
                    let mut buf = vec![0u8; 8192];
                    let n = sock.read(&mut buf).await.unwrap_or(0);
                    let req = String::from_utf8_lossy(&buf[..n]).to_string();
                    let path = req
                        .lines()
                        .next()
                        .and_then(|l| l.split_whitespace().nth(1))
                        .unwrap_or("/")
                        .to_string();
                    seen.lock().map(|mut g| g.push(path.clone())).ok();
                    let body = if path.contains("_mapping") {
                        json!({"logs-2026": {"mappings": {"properties": {
                            "@timestamp": {"type": "date"},
                            "message": {"type": "text"},
                            "http": {"properties": {"status": {"type": "integer"}}},
                            "count": {"type": "long"},
                            "ratio": {"type": "double"},
                            "flag": {"type": "boolean"}
                        }}}})
                    } else if path.contains("_count") {
                        json!({"count": 3})
                    } else if path.contains("_stats") {
                        json!({"indices": {"logs-2026": {"primaries": {"store": {"size_in_bytes": 12345}}}}})
                    } else if path.contains("_search") {
                        let mut guard = pages.lock().expect("pages");
                        if guard.is_empty() {
                            json!({"_scroll_id": "done", "hits": {"hits": []}})
                        } else {
                            guard.remove(0)
                        }
                    } else {
                        json!({"acknowledged": true})
                    };
                    let payload = body.to_string();
                    let resp = format!(
                        "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
                        payload.len(),
                        payload
                    );
                    let _ = sock.write_all(resp.as_bytes()).await;
                    let _ = sock.shutdown().await;
                });
            }
        });
        (format!("http://{addr}"), seen, handle)
    }

    fn hit(ts: &str, status: i64) -> Json {
        json!({
            "_source": {
                "@timestamp": ts,
                "message": "hello",
                "http": {"status": status},
                "count": 7,
                "ratio": 0.25,
                "flag": true
            }
        })
    }

    #[tokio::test]
    async fn reads_mapping_scrolls_and_drops_context() {
        let pages = vec![
            json!({"_scroll_id": "s1", "hits": {"hits": [hit("2026-11-21T10:00:00Z", 200)]}}),
            json!({"_scroll_id": "s2", "hits": {"hits": [hit("2026-11-21T10:00:01Z", 500)]}}),
            json!({"_scroll_id": "s3", "hits": {"hits": []}}),
        ];
        let (base, seen, handle) = stub_es(pages).await;
        let dir = std::env::temp_dir().join(format!("atp-es-{}", std::process::id()));
        let mut cfg = SourceConfig::new(SourceKind::Es, base);
        cfg.timestamp_column = Some("@timestamp".to_string());
        cfg.es = EsConfig::new("logs-2026", dir.clone());
        let mut src = EsSource::new(&cfg).expect("new");
        let schema = src.infer().await.expect("infer");
        assert_eq!(schema.index_of("@timestamp"), Some(0));
        assert_eq!(
            schema.fields[schema.index_of("http.status").expect("status")].ty,
            Ty::I64
        );
        let info = src.describe();
        assert_eq!(info.rows_estimate, Some(3));
        assert_eq!(info.bytes_estimate, Some(12345));

        let p1 = src.next_page(1000).await.expect("page").expect("some");
        assert_eq!(p1.rows.len(), 1);
        assert!(dir.join("checkpoint-logs-2026.json").exists());
        let p2 = src.next_page(1000).await.expect("page").expect("some");
        assert_eq!(p2.rows.len(), 1);
        assert!(src.next_page(1000).await.expect("page").is_none());
        src.finish().await.expect("finish");
        assert!(!dir.join("checkpoint-logs-2026.json").exists());
        let paths = seen.lock().expect("seen").clone();
        assert!(paths.iter().any(|p| p.contains("_mapping")));
        assert!(paths.iter().any(|p| p.contains("_search/scroll")));
        handle.abort();
    }

    #[tokio::test]
    async fn resume_uses_checkpoint_slice_and_scroll_id() {
        let pages =
            vec![json!({"_scroll_id": "s9", "hits": {"hits": [hit("2026-11-21T10:00:00Z", 200)]}})];
        let (base, seen, handle) = stub_es(pages).await;
        let dir = std::env::temp_dir().join(format!("atp-es-resume-{}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("dir");
        let mut cfg = SourceConfig::new(SourceKind::Es, base);
        cfg.es = EsConfig::new("logs-2026", dir.clone());
        cfg.es.resume = true;
        cfg.es.slices = 2;
        let probe = EsSource::new(&cfg).expect("probe");
        let cp = Checkpoint {
            index: "logs-2026".into(),
            query_hash: probe.query_hash.clone(),
            slice: 1,
            slices: 2,
            scroll_id: Some("s8".into()),
            rows_fetched: 10,
        };
        std::fs::write(
            dir.join("checkpoint-logs-2026.json"),
            serde_json::to_vec(&cp).expect("json"),
        )
        .expect("cp");

        let mut src = EsSource::new(&cfg).expect("new");
        src.infer().await.expect("infer");
        let page = src.next_page(1000).await.expect("page").expect("some");
        assert_eq!(page.rows.len(), 1);
        let paths = seen.lock().expect("seen").clone();
        // Only the scroll endpoint was needed; no fresh _search with slice 0.
        assert!(paths.iter().any(|p| p.contains("_search/scroll")));
        assert!(!paths.iter().any(|p| p.contains("?scroll=")));
        handle.abort();
    }

    #[test]
    fn mapping_types_merge_across_indices() {
        let mut out = BTreeMap::new();
        let mut props = Map::new();
        props.insert("a".to_string(), json!({"type": "long"}));
        collect_properties("", &props, &mut out);
        let mut props2 = Map::new();
        props2.insert("a".to_string(), json!({"type": "keyword"}));
        collect_properties("", &props2, &mut out);
        assert_eq!(out["a"], Ty::Str);
    }

    #[test]
    fn url_is_built_without_double_slashes() {
        let dir = std::env::temp_dir();
        let mut cfg = SourceConfig::new(SourceKind::Es, "http://es:9200/");
        cfg.es = EsConfig::new("logs-*", dir);
        let src = EsSource::new(&cfg).expect("new");
        assert_eq!(src.url("_mapping"), "http://es:9200/logs-*/_mapping");
    }

    #[test]
    fn checkpoint_path_is_sanitized() {
        assert_eq!(sanitize("logs-*"), "logs-_");
    }
}
