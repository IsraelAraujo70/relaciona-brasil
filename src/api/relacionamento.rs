use std::collections::{HashMap, HashSet};

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::response::Response;
use axum::routing::get;
use axum::{Json, Router};
use chrono::NaiveDate;
use serde::{Deserialize, Serialize};
use utoipa::{IntoParams, ToSchema};

use super::AppState;
use crate::domain::types::Doc;
use crate::error::AppError;
use crate::jobs::queue::{self, EnqueueRequest};

pub fn router() -> Router<AppState> {
    Router::new().route("/v1/relacionamento/{doc}", get(grafo))
}

#[derive(Debug, Deserialize, IntoParams)]
pub struct ProfundidadeQuery {
    /// Número de saltos a expandir (1-4, default 2). Cada salto alterna pessoa↔empresa.
    #[serde(default = "default_profundidade")]
    pub profundidade: u32,

    /// Quando o cache não tem a resposta, a API enfileira um job e devolve
    /// 202. Se `callback` for informado, o worker faz POST aqui ao concluir.
    pub callback: Option<String>,
}

fn default_profundidade() -> u32 {
    2
}

#[derive(Debug, Serialize, ToSchema)]
pub struct No {
    /// CPF mascarado, CNPJ completo ou cnpj_basico (8 chars).
    pub id: String,
    /// `"pessoa"` ou `"empresa"`.
    pub tipo: String,
    /// `nome_socio` (pessoa) ou `razao_social` (empresa).
    pub nome: Option<String>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct Aresta {
    pub de: String,
    pub para: String,
    pub qualificacao: Option<i16>,
    pub desde: Option<NaiveDate>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct Raiz {
    pub id: String,
    pub tipo: String,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct Grafo {
    pub raiz: Raiz,
    pub profundidade: i32,
    pub nos: Vec<No>,
    pub arestas: Vec<Aresta>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct JobAceito {
    pub job_id: uuid::Uuid,
    pub status: String,
    pub poll_url: String,
    pub mensagem: String,
}

#[derive(Debug, sqlx::FromRow)]
struct WalkRow {
    id: String,
    tipo: String,
    de_id: Option<String>,
    para_id: Option<String>,
    qualificacao: Option<i16>,
    data_entrada: Option<NaiveDate>,
    nome_socio: Option<String>,
}

#[utoipa::path(
    get,
    path = "/v1/relacionamento/{doc}",
    tag = "relacionamento",
    params(
        ("doc" = String, Path, description = "CPF (11 dígitos), CPF mascarado (***NNNNNN**) ou CNPJ (14 dígitos)"),
        ProfundidadeQuery,
    ),
    responses(
        (status = 200, description = "Grafo de relacionamentos (cache hit)", body = Grafo),
        (status = 202, description = "Job enfileirado — consulte poll_url ou aguarde callback", body = JobAceito),
        (status = 400, description = "Documento inválido"),
    ),
)]
pub async fn grafo(
    State(state): State<AppState>,
    Path(doc_str): Path<String>,
    Query(q): Query<ProfundidadeQuery>,
) -> Result<Response, AppError> {
    let doc = Doc::parse(&doc_str).map_err(|e| AppError::BadRequest(e.into()))?;
    let profundidade = q.profundidade.clamp(1, 4) as i32;

    let (raiz_id, raiz_tipo): (String, String) = match &doc {
        Doc::Cnpj(c) => (c.basico().to_string(), "empresa".into()),
        Doc::Cpf(c) => (c.masked().to_string(), "pessoa".into()),
    };

    if let Some(grafo) = build_grafo_from_cache(&state, &raiz_id, &raiz_tipo, profundidade).await? {
        return Ok((StatusCode::OK, Json(grafo)).into_response());
    }

    // CPF não dispara scan: o dump é indexado por cnpj_basico, não há reverse
    // lookup eficiente nos zips. Documentar.
    let Doc::Cnpj(cnpj) = &doc else {
        return Err(AppError::BadRequest(
            "CPF sem dados em cache; lookup por CPF requer pré-população via consulta a CNPJs relacionados".into(),
        ));
    };

    let req = EnqueueRequest {
        cnpj_basico: cnpj.basico().to_string(),
        profundidade: profundidade as i16,
        callback_url: q.callback,
    };
    let (job, _outcome) = queue::enqueue(&state.pool, req)
        .await
        .map_err(AppError::Other)?;

    let aceito = JobAceito {
        job_id: job.id,
        status: job.status.clone(),
        poll_url: format!("/v1/jobs/{}", job.id),
        mensagem: "consulta enfileirada — primeira leitura é fria, aguarde o worker".into(),
    };
    Ok((StatusCode::ACCEPTED, Json(aceito)).into_response())
}

async fn build_grafo_from_cache(
    state: &AppState,
    raiz_id: &str,
    raiz_tipo: &str,
    profundidade: i32,
) -> Result<Option<Grafo>, AppError> {
    let walk: Vec<WalkRow> = sqlx::query_as(
        "WITH RECURSIVE walk AS ( \
           SELECT 0::int AS nivel, \
                  $1::text AS id, \
                  $2::text AS tipo, \
                  NULL::text AS de_id, \
                  NULL::text AS para_id, \
                  NULL::int2 AS qualificacao, \
                  NULL::date AS data_entrada, \
                  NULL::text AS nome_socio \
           UNION \
           SELECT w.nivel + 1, \
                  CASE WHEN w.tipo = 'empresa' THEN s.cnpj_cpf_socio ELSE s.cnpj_basico END, \
                  CASE WHEN w.tipo = 'empresa' THEN 'pessoa' ELSE 'empresa' END, \
                  CASE WHEN w.tipo = 'empresa' THEN s.cnpj_basico ELSE s.cnpj_cpf_socio END, \
                  CASE WHEN w.tipo = 'empresa' THEN s.cnpj_cpf_socio ELSE s.cnpj_basico END, \
                  s.qualificacao, \
                  s.data_entrada, \
                  s.nome_socio \
           FROM walk w \
           JOIN socio s ON \
             (w.tipo = 'empresa' AND s.cnpj_basico = w.id) \
             OR (w.tipo = 'pessoa' AND s.cnpj_cpf_socio = w.id) \
           WHERE w.nivel < $3 \
         ) \
         SELECT id, tipo, de_id, para_id, qualificacao, data_entrada, nome_socio \
         FROM walk",
    )
    .bind(raiz_id)
    .bind(raiz_tipo)
    .bind(profundidade)
    .fetch_all(&state.pool)
    .await?;

    // Sem nenhum vínculo encontrado para essa raiz no cache → não temos resposta.
    let has_arestas = walk.iter().any(|r| r.de_id.is_some());
    if !has_arestas {
        return Ok(None);
    }

    let mut nos: HashMap<(String, String), Option<String>> = HashMap::new();
    let mut arestas: HashSet<(String, String, Option<i16>, Option<NaiveDate>)> = HashSet::new();

    for row in &walk {
        let key = (row.id.clone(), row.tipo.clone());
        let entry = nos.entry(key).or_insert(None);
        if entry.is_none() && row.tipo == "pessoa" {
            *entry = row.nome_socio.clone();
        }
        if let (Some(de), Some(para)) = (&row.de_id, &row.para_id) {
            arestas.insert((de.clone(), para.clone(), row.qualificacao, row.data_entrada));
        }
    }

    let cnpjs_basico: Vec<String> = nos
        .keys()
        .filter(|(_, t)| t == "empresa")
        .map(|(id, _)| id.clone())
        .collect();

    if !cnpjs_basico.is_empty() {
        let empresas: Vec<(String, Option<String>)> = sqlx::query_as(
            "SELECT cnpj_basico, razao_social FROM empresa WHERE cnpj_basico = ANY($1)",
        )
        .bind(&cnpjs_basico)
        .fetch_all(&state.pool)
        .await?;
        for (cnpj_basico, razao) in empresas {
            if let Some(slot) = nos.get_mut(&(cnpj_basico.clone(), "empresa".into())) {
                *slot = razao;
            }
        }
    }

    let nos_out: Vec<No> = nos
        .into_iter()
        .map(|(k, nome)| No {
            id: k.0,
            tipo: k.1,
            nome,
        })
        .collect();
    let arestas_out: Vec<Aresta> = arestas
        .into_iter()
        .map(|(de, para, q, d)| Aresta {
            de,
            para,
            qualificacao: q,
            desde: d,
        })
        .collect();

    Ok(Some(Grafo {
        raiz: Raiz {
            id: raiz_id.to_string(),
            tipo: raiz_tipo.to_string(),
        },
        profundidade,
        nos: nos_out,
        arestas: arestas_out,
    }))
}
