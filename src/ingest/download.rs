//! Download streaming de arquivos do share Nextcloud — não buffera no disco
//! nem na RAM, transforma o response em `AsyncRead` que alimenta o unzip.

use anyhow::{Context, Result};
use futures::TryStreamExt;
use tokio::io::AsyncRead;
use tokio_util::io::StreamReader;

use super::source::Source;

pub struct Download {
    pub size_bytes: Option<u64>,
    pub reader: Box<dyn AsyncRead + Send + Unpin>,
}

pub async fn fetch(source: &Source, url: &str) -> Result<Download> {
    let resp = source
        .client()
        .get(url)
        .basic_auth(source.token(), Some(""))
        .send()
        .await
        .with_context(|| format!("GET {url}"))?
        .error_for_status()?;

    let size_bytes = resp.content_length();

    let stream = resp.bytes_stream().map_err(std::io::Error::other);

    Ok(Download {
        size_bytes,
        reader: Box::new(StreamReader::new(stream)),
    })
}
