//! Resolve URLs e descobre o vintage mais recente no compartilhamento Nextcloud
//! da Receita Federal. A API pública usa WebDAV em `/public.php/webdav/` com
//! basic auth onde o usuário é o token do share e a senha é vazia.

use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use reqwest::Method;

#[derive(Debug, Clone)]
pub struct Source {
    base_url: String,
    token: String,
    client: reqwest::Client,
}

#[derive(Debug, Clone)]
pub struct Entry {
    pub name: String,
    pub is_dir: bool,
    pub size: u64,
}

impl Source {
    /// Constrói a partir da URL pública do share, ex.:
    /// `https://arquivos.receitafederal.gov.br/index.php/s/YggdBLfdninEJX9`
    pub fn from_share_url(share_url: &str) -> Result<Self> {
        let trimmed = share_url.trim_end_matches('/');
        let token = trimmed
            .rsplit('/')
            .next()
            .filter(|t| !t.is_empty())
            .ok_or_else(|| anyhow!("URL de compartilhamento sem token: {share_url}"))?
            .to_string();
        let base_url = trimmed
            .split("/index.php/")
            .next()
            .ok_or_else(|| anyhow!("URL inválida: {share_url}"))?
            .to_string();

        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(60 * 60))
            .connect_timeout(Duration::from_secs(30))
            .build()?;

        Ok(Self {
            base_url,
            token,
            client,
        })
    }

    pub fn token(&self) -> &str {
        &self.token
    }

    pub fn client(&self) -> &reqwest::Client {
        &self.client
    }

    pub fn file_url(&self, vintage: &str, filename: &str) -> String {
        format!(
            "{}/public.php/webdav/{}/{}",
            self.base_url, vintage, filename
        )
    }

    /// Lista um subdiretório (string vazia para a raiz). Usa PROPFIND Depth: 1.
    pub async fn list(&self, subpath: &str) -> Result<Vec<Entry>> {
        let subpath = subpath.trim_matches('/');
        let url = if subpath.is_empty() {
            format!("{}/public.php/webdav/", self.base_url)
        } else {
            format!("{}/public.php/webdav/{}/", self.base_url, subpath)
        };

        let body = self
            .client
            .request(Method::from_bytes(b"PROPFIND")?, &url)
            .basic_auth(&self.token, Some(""))
            .header("Depth", "1")
            .send()
            .await
            .with_context(|| format!("PROPFIND {url}"))?
            .error_for_status()?
            .text()
            .await?;

        Ok(parse_propfind(&body, subpath))
    }

    /// Pega a pasta `YYYY-MM` mais recente.
    pub async fn latest_vintage(&self) -> Result<String> {
        self.list("")
            .await?
            .into_iter()
            .filter(|e| e.is_dir && is_vintage(&e.name))
            .map(|e| e.name)
            .max()
            .ok_or_else(|| anyhow!("nenhuma pasta YYYY-MM no compartilhamento"))
    }
}

fn is_vintage(name: &str) -> bool {
    let bytes = name.as_bytes();
    bytes.len() == 7
        && bytes[4] == b'-'
        && bytes[..4].iter().all(|c| c.is_ascii_digit())
        && bytes[5..].iter().all(|c| c.is_ascii_digit())
}

fn parse_propfind(xml: &str, listed_subpath: &str) -> Vec<Entry> {
    let mut entries = Vec::new();
    for resp in xml.split("<d:response>").skip(1) {
        let Some(href) = capture(resp, "<d:href>", "</d:href>") else {
            continue;
        };
        // remove `/public.php/webdav/` prefix
        let after_root = href
            .splitn(4, '/')
            .nth(3)
            .unwrap_or("")
            .trim_end_matches('/');

        // pula a entrada da própria pasta listada
        if after_root.is_empty() || after_root == listed_subpath {
            continue;
        }

        let name = after_root.rsplit('/').next().unwrap_or("").to_string();
        if name.is_empty() {
            continue;
        }

        let is_dir = resp.contains("<d:collection/>");
        let size = capture(resp, "<d:getcontentlength>", "</d:getcontentlength>")
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);

        entries.push(Entry { name, is_dir, size });
    }
    entries
}

fn capture<'a>(s: &'a str, start: &str, end: &str) -> Option<&'a str> {
    let i = s.find(start)? + start.len();
    let rest = &s[i..];
    let j = rest.find(end)?;
    Some(&rest[..j])
}

#[cfg(test)]
mod tests {
    use super::*;

    const SAMPLE: &str = r#"<?xml version="1.0"?>
<d:multistatus xmlns:d="DAV:">
<d:response><d:href>/public.php/webdav/2026-04/</d:href><d:propstat><d:prop><d:resourcetype><d:collection/></d:resourcetype></d:prop></d:propstat></d:response>
<d:response><d:href>/public.php/webdav/2026-04/Cnaes.zip</d:href><d:propstat><d:prop><d:getcontentlength>12345</d:getcontentlength><d:resourcetype/></d:prop></d:propstat></d:response>
<d:response><d:href>/public.php/webdav/2026-04/Empresas0.zip</d:href><d:propstat><d:prop><d:getcontentlength>518000000</d:getcontentlength><d:resourcetype/></d:prop></d:propstat></d:response>
</d:multistatus>"#;

    #[test]
    fn parse_propfind_skips_self_and_extracts_files() {
        let entries = parse_propfind(SAMPLE, "2026-04");
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].name, "Cnaes.zip");
        assert_eq!(entries[0].size, 12_345);
        assert!(!entries[0].is_dir);
        assert_eq!(entries[1].name, "Empresas0.zip");
        assert_eq!(entries[1].size, 518_000_000);
    }

    #[test]
    fn vintage_format() {
        assert!(is_vintage("2026-04"));
        assert!(is_vintage("2023-05"));
        assert!(!is_vintage("2026-4"));
        assert!(!is_vintage("Cnaes.zip"));
        assert!(!is_vintage("2026/04"));
    }

    #[test]
    fn from_share_url_parses_token_and_base() {
        let s = Source::from_share_url(
            "https://arquivos.receitafederal.gov.br/index.php/s/YggdBLfdninEJX9",
        )
        .unwrap();
        assert_eq!(s.token, "YggdBLfdninEJX9");
        assert_eq!(s.base_url, "https://arquivos.receitafederal.gov.br");
        assert_eq!(
            s.file_url("2026-04", "Cnaes.zip"),
            "https://arquivos.receitafederal.gov.br/public.php/webdav/2026-04/Cnaes.zip"
        );
    }
}
