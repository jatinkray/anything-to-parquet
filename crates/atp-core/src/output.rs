//! Where a run writes: a local directory or an object-store prefix.
//!
//! Object-store runs are staged locally, file by file, and each finished Parquet
//! file is uploaded before the run continues. That keeps the proven local sink
//! (`.tmp` + rename, hash while writing) as the single writer and gives object
//! stores the only atomicity primitive they have: a completed `PUT`/multipart
//! upload. Peak scratch space is one output tree; a crash leaves uploaded files
//! plus a manifest that says what was durable.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use object_store::path::Path as ObjectPath;
use object_store::{ObjectStore, ObjectStoreExt};

use crate::error::{Error, Result};

/// A parsed output target. `Remote` carries no client so parsing stays cheap and
/// testable; [`RemoteOutput::connect`] builds the provider from the environment.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Output {
    Local(PathBuf),
    Remote(RemoteOutput),
}

impl Output {
    /// `s3://bucket/prefix`, `gs://bucket/prefix`, `az://container/prefix` are
    /// object-store targets; everything else is a local directory.
    pub fn parse(out: &Path) -> Result<Output> {
        let text = out.to_string_lossy();
        match RemoteOutput::parse(&text)? {
            Some(remote) => Ok(Output::Remote(remote)),
            None => Ok(Output::Local(out.to_path_buf())),
        }
    }

    pub fn is_remote(&self) -> bool {
        matches!(self, Output::Remote(_))
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RemoteOutput {
    pub scheme: String,
    pub bucket: String,
    /// Prefix inside the bucket, without leading or trailing `/`.
    pub prefix: String,
    /// Canonical `scheme://bucket[/prefix]` for progress output and manifests.
    pub display: String,
}

impl RemoteOutput {
    pub fn parse(raw: &str) -> Result<Option<RemoteOutput>> {
        let Some((scheme, rest)) = raw.split_once("://") else {
            return Ok(None);
        };
        let scheme = scheme.to_ascii_lowercase();
        if !matches!(scheme.as_str(), "s3" | "gs" | "az") {
            return Err(Error::msg(format!(
                "unsupported output scheme `{scheme}://`; use s3://, gs://, az:// or a local directory"
            )));
        }
        let (bucket, prefix) = rest.split_once('/').unwrap_or((rest, ""));
        let bucket = bucket.trim();
        if bucket.is_empty() {
            return Err(Error::msg(format!(
                "output `{raw}` has no bucket/container name"
            )));
        }
        let prefix = prefix.trim_matches('/');
        let display = if prefix.is_empty() {
            format!("{scheme}://{bucket}")
        } else {
            format!("{scheme}://{bucket}/{prefix}")
        };
        Ok(Some(RemoteOutput {
            scheme,
            bucket: bucket.to_string(),
            prefix: prefix.to_string(),
            display,
        }))
    }

    /// Build the provider from the standard environment (`AWS_*`, `GOOGLE_*`,
    /// `AZURE_*`). Credentials are resolved by `object_store` at request time.
    pub fn connect(&self) -> Result<RemoteStore> {
        let store: Arc<dyn ObjectStore> = match self.scheme.as_str() {
            "s3" => Arc::new(
                object_store::aws::AmazonS3Builder::from_env()
                    .with_bucket_name(&self.bucket)
                    .build()
                    .map_err(|error| Error::object_store(format!("opening S3 output: {error}")))?,
            ),
            "gs" => Arc::new(
                object_store::gcp::GoogleCloudStorageBuilder::from_env()
                    .with_bucket_name(&self.bucket)
                    .build()
                    .map_err(|error| Error::object_store(format!("opening GCS output: {error}")))?,
            ),
            "az" => Arc::new(
                object_store::azure::MicrosoftAzureBuilder::from_env()
                    .with_container_name(&self.bucket)
                    .build()
                    .map_err(|error| {
                        Error::object_store(format!("opening Azure output: {error}"))
                    })?,
            ),
            other => {
                return Err(Error::msg(format!(
                    "unsupported output scheme `{other}://`"
                )))
            }
        };
        Ok(RemoteStore {
            store,
            prefix: ObjectPath::from(self.prefix.as_str()),
            display: self.display.clone(),
        })
    }
}

/// A connected object-store prefix. Every path is relative to it.
pub struct RemoteStore {
    store: Arc<dyn ObjectStore>,
    prefix: ObjectPath,
    display: String,
}

impl RemoteStore {
    pub fn display(&self) -> &str {
        &self.display
    }

    fn object_path(&self, relative: &str) -> ObjectPath {
        if self.prefix.as_ref().is_empty() {
            ObjectPath::from(relative)
        } else {
            ObjectPath::from(format!("{}/{}", self.prefix, relative))
        }
    }

    /// Upload one finished local file, streaming with multipart above 10 MiB and
    /// a single `PUT` below it. The object appears only once the upload commits.
    pub async fn put_file(&self, local: &Path, relative: &str) -> Result<()> {
        use tokio::io::AsyncWriteExt;

        let file = tokio::fs::File::open(local)
            .await
            .map_err(|error| Error::Path {
                action: "open for upload",
                path: local.to_path_buf(),
                reason: error.to_string(),
            })?;
        let mut reader = tokio::io::BufReader::with_capacity(256 * 1024, file);
        let path = self.object_path(relative);
        let mut writer =
            object_store::buffered::BufWriter::new(Arc::clone(&self.store), path.clone());
        tokio::io::copy(&mut reader, &mut writer)
            .await
            .map_err(|error| {
                Error::object_store(format!("uploading {}: {error}", self.display_key(&path)))
            })?;
        writer.shutdown().await.map_err(|error| {
            Error::object_store(format!("finishing {}: {error}", self.display_key(&path)))
        })
    }

    /// Upload a small object (the manifest) in one request.
    pub async fn put_bytes(&self, relative: &str, bytes: Vec<u8>) -> Result<()> {
        let path = self.object_path(relative);
        self.store.put(&path, bytes.into()).await.map_err(|error| {
            Error::object_store(format!("uploading {}: {error}", self.display_key(&path)))
        })?;
        Ok(())
    }

    fn display_key(&self, path: &ObjectPath) -> String {
        format!("{}/{}", self.display, path)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn local_paths_stay_local() {
        let out = Output::parse(Path::new("/tmp/lake")).expect("parse");
        assert_eq!(out, Output::Local(PathBuf::from("/tmp/lake")));
        assert!(!out.is_remote());
    }

    #[test]
    fn s3_urls_split_bucket_and_prefix() {
        let out = Output::parse(Path::new("s3://lake/metrics/prod"))
            .expect("parse")
            .remote_for_test();
        assert_eq!(out.scheme, "s3");
        assert_eq!(out.bucket, "lake");
        assert_eq!(out.prefix, "metrics/prod");
        assert_eq!(out.display, "s3://lake/metrics/prod");
    }

    #[test]
    fn bucket_without_prefix_and_trailing_slashes_are_normalised() {
        let bare = RemoteOutput::parse("gs://bucket")
            .expect("parse")
            .expect("remote");
        assert_eq!(bare.prefix, "");
        assert_eq!(bare.display, "gs://bucket");

        let trailing = RemoteOutput::parse("az://container/a/b/")
            .expect("parse")
            .expect("remote");
        assert_eq!(trailing.prefix, "a/b");
        assert_eq!(trailing.display, "az://container/a/b");
    }

    #[test]
    fn unknown_scheme_and_missing_bucket_are_errors() {
        assert!(RemoteOutput::parse("ftp://host/path").is_err());
        assert!(RemoteOutput::parse("s3://").is_err());
        assert!(RemoteOutput::parse("s3:///prefix").is_err());
    }

    impl Output {
        fn remote_for_test(self) -> RemoteOutput {
            match self {
                Output::Remote(remote) => remote,
                Output::Local(path) => panic!("expected remote output, got {path:?}"),
            }
        }
    }
}
