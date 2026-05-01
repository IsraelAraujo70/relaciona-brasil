use std::collections::{HashMap, HashSet};

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::get;
use axum::{Json, Router};
use chrono::NaiveDate;
use serde::{Deserialize, Serialize};
use serde_json::json;

use super::AppState;
use crate::domain::types::Doc;
use crate::error::AppError;

pub fn router() -> Router<AppState> {
    Router::new().route("/v1/relacionamento/{doc}", get(grafo))
}

#[derive(Debug, Deserialize)]
struct ProfundidadeQuery {
    #[serde(default = "default_profundidade")]
    profundidade: u32,
}

fn default_profundidade() -> u32 {
    2
}

#[derive(Debug, Serialize)]
struct No {
    id: String,
    tipo: String,
    nome: Option<String>,
}

#[derive(Debug, Serialize)]
struct Aresta {
    de: String,
    para: String,
    qualificacao: Option<i16>,
    desde: Option<NaiveDate>,
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

async fn grafo(
    State(state): State<AppState>,
    Path(doc_str): Path<String>,
    Query(q): Query<ProfundidadeQuery>,
) -> Result<impl IntoResponse, AppError> {
    let doc = Doc::parse(&doc_str).map_err(|e| AppError::BadRequest(e.into()))?;
    let profundidade = q.profundidade.clamp(1, 4) as i32;

    // Normaliza o id raiz e o tipo
    let (raiz_id, raiz_tipo): (String, String) = match &doc {
        Doc::Cnpj(c) => (c.basico().to_string(), "empresa".into()),
        Doc::Cpf(c) => (c.masked().to_string(), "pessoa".into()),
    };

    // Recursive CTE: walk pelo grafo socio (pessoa↔empresa).
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
    .bind(&raiz_id)
    .bind(&raiz_tipo)
    .bind(profundidade)
    .fetch_all(&state.pool)
    .await?;

    // Coletar nodes únicos e edges
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

    // Buscar razao_social das empresas para preencher o nome
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
        .map(|((id, tipo), nome)| No { id, tipo, nome })
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

    Ok((
        StatusCode::OK,
        Json(json!({
            "raiz": { "id": raiz_id, "tipo": raiz_tipo },
            "profundidade": profundidade,
            "nos": nos_out,
            "arestas": arestas_out,
        })),
    ))
}
