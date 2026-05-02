//! Entrega de callback HTTP quando um job termina. POST JSON com retry
//! exponencial — falha de delivery NÃO invalida o job (já está completed).

use std::time::Duration;

use anyhow::{anyhow, Result};
use serde::Serialize;
use uuid::Uuid;

#[derive(Debug, Serialize)]
pub struct WebhookPayload<'a> {
    pub job_id: Uuid,
    pub status: &'a str,
    pub resultado: Option<&'a serde_json::Value>,
    pub erro: Option<&'a str>,
}

const RETRY_BACKOFFS: [Duration; 3] = [
    Duration::from_secs(5),
    Duration::from_secs(30),
    Duration::from_secs(120),
];

pub async fn deliver(callback_url: &str, payload: &WebhookPayload<'_>) -> Result<()> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()?;

    let mut last_err: Option<anyhow::Error> = None;
    for (attempt, backoff) in std::iter::once(Duration::ZERO)
        .chain(RETRY_BACKOFFS.iter().copied())
        .enumerate()
    {
        if !backoff.is_zero() {
            tokio::time::sleep(backoff).await;
        }

        match client
            .post(callback_url)
            .header("Content-Type", "application/json")
            .json(payload)
            .send()
            .await
        {
            Ok(resp) if resp.status().is_success() => return Ok(()),
            Ok(resp) => {
                last_err = Some(anyhow!(
                    "webhook attempt {} returned {}",
                    attempt + 1,
                    resp.status()
                ));
            }
            Err(err) => {
                last_err = Some(anyhow!("webhook attempt {}: {}", attempt + 1, err));
            }
        }
    }

    Err(last_err.unwrap_or_else(|| anyhow!("webhook delivery failed without error")))
}
