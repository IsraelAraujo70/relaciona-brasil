//! Carga em massa para Postgres.
//!
//! - Lookups (poucas linhas) usam UNNEST INSERT em transação com TRUNCATE CASCADE.
//! - Tabelas grandes (empresa, estabelecimento, sócio, simples) usam
//!   `COPY ... FROM STDIN` em formato TEXT, alimentado por um stream tipado
//!   vindo do parser CSV.

use anyhow::Result;
use futures::StreamExt;
use sqlx::postgres::PgPoolCopyExt;
use sqlx::PgPool;
use tokio::io::AsyncRead;

use super::parse::{self, EmpresaRow, EstabelecimentoRow, LookupRow, SimplesRow, SocioRow};

const COPY_BUF_FLUSH: usize = 256 * 1024;

pub async fn replace_lookup(
    pool: &PgPool,
    table: &str,
    codigo_type: &str,
    rows: &[LookupRow],
) -> Result<u64> {
    if rows.is_empty() {
        return Ok(0);
    }

    let mut tx = pool.begin().await?;

    sqlx::query(&format!("TRUNCATE TABLE {table} CASCADE"))
        .execute(&mut *tx)
        .await?;

    let codigos: Vec<&str> = rows.iter().map(|r| r.codigo.as_str()).collect();
    let descricoes: Vec<&str> = rows.iter().map(|r| r.descricao.as_str()).collect();

    let sql = format!(
        "INSERT INTO {table} (codigo, descricao) \
         SELECT codigo::{codigo_type}, descricao \
         FROM UNNEST($1::text[], $2::text[]) AS t(codigo, descricao)"
    );

    let inserted = sqlx::query(&sql)
        .bind(&codigos)
        .bind(&descricoes)
        .execute(&mut *tx)
        .await?
        .rows_affected();

    tx.commit().await?;
    Ok(inserted)
}

/// Trunca a tabela `empresa` em CASCADE (também esvazia estabelecimento, sócio, simples).
pub async fn truncate_empresa_cascade(pool: &PgPool) -> Result<()> {
    sqlx::query("TRUNCATE TABLE empresa CASCADE")
        .execute(pool)
        .await?;
    Ok(())
}

/// Drop/recreate da FK estabelecimento → empresa em torno da carga em massa.
/// O COPY fica significativamente mais rápido sem a verificação de FK; a
/// limpeza de órfãos roda explicitamente depois e a FK volta no fim.
pub async fn drop_fk_estab(pool: &PgPool) -> Result<()> {
    sqlx::query(
        "ALTER TABLE estabelecimento \
         DROP CONSTRAINT IF EXISTS estabelecimento_cnpj_basico_fkey",
    )
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn recreate_fk_estab(pool: &PgPool) -> Result<()> {
    sqlx::query(
        "ALTER TABLE estabelecimento \
         ADD CONSTRAINT estabelecimento_cnpj_basico_fkey \
         FOREIGN KEY (cnpj_basico) REFERENCES empresa(cnpj_basico) ON DELETE CASCADE",
    )
    .execute(pool)
    .await?;
    Ok(())
}

/// Remove estabelecimentos cuja empresa não existe (caso edge — smoke test
/// com max-files parcial, ou inconsistências do dump).
pub async fn delete_orphan_estab(pool: &PgPool) -> Result<u64> {
    let res = sqlx::query(
        "DELETE FROM estabelecimento e \
         WHERE NOT EXISTS (SELECT 1 FROM empresa WHERE cnpj_basico = e.cnpj_basico)",
    )
    .execute(pool)
    .await?;
    Ok(res.rows_affected())
}

/// Lê EstabelecimentoRow de um CSV streaming, mantém apenas os com `situacao='02'`
/// e despeja no Postgres via COPY.
pub async fn copy_estabelecimentos<R: AsyncRead + Unpin + Send + 'static>(
    pool: &PgPool,
    input: R,
) -> Result<(u64, u64)> {
    let mut rdr = parse::make_deserializer(input);
    let mut rows = rdr.deserialize::<EstabelecimentoRow>();

    let mut copy_in = pool
        .copy_in_raw(
            "COPY estabelecimento (cnpj_basico, cnpj_ordem, cnpj_dv, matriz_filial, \
             nome_fantasia, situacao, data_situacao, motivo_situacao, nome_cidade_ext, \
             pais, data_inicio, cnae_principal, cnaes_secundarios, tipo_logradouro, \
             logradouro, numero, complemento, bairro, cep, uf, municipio, ddd_1, \
             telefone_1, ddd_2, telefone_2, ddd_fax, fax, email, situacao_especial, \
             data_sit_especial) \
             FROM STDIN",
        )
        .await?;

    let mut buf: Vec<u8> = Vec::with_capacity(COPY_BUF_FLUSH);
    let mut total: u64 = 0;
    let mut kept: u64 = 0;

    while let Some(row) = rows.next().await {
        let row = row?;
        total += 1;
        if row.situacao != "02" {
            continue;
        }
        row.write_copy_line(&mut buf);
        kept += 1;
        if buf.len() >= COPY_BUF_FLUSH {
            copy_in.send(buf.as_slice()).await?;
            buf.clear();
        }
    }
    if !buf.is_empty() {
        copy_in.send(buf.as_slice()).await?;
    }
    copy_in.finish().await?;
    Ok((total, kept))
}

// ─── Manutenção de índices (drop antes de COPY, recreate depois) ────────────

pub async fn drop_estab_indexes(pool: &PgPool) -> Result<()> {
    for idx in [
        "idx_estab_cnpj",
        "idx_estab_municipio",
        "idx_estab_cnae",
        "idx_estab_uf",
    ] {
        sqlx::query(&format!("DROP INDEX IF EXISTS {idx}"))
            .execute(pool)
            .await?;
    }
    Ok(())
}

pub async fn recreate_estab_indexes(pool: &PgPool) -> Result<()> {
    for sql in [
        "CREATE INDEX IF NOT EXISTS idx_estab_cnpj      ON estabelecimento (cnpj)",
        "CREATE INDEX IF NOT EXISTS idx_estab_municipio ON estabelecimento (municipio)",
        "CREATE INDEX IF NOT EXISTS idx_estab_cnae      ON estabelecimento (cnae_principal)",
        "CREATE INDEX IF NOT EXISTS idx_estab_uf        ON estabelecimento (uf)",
    ] {
        sqlx::query(sql).execute(pool).await?;
    }
    Ok(())
}

pub async fn drop_socio_indexes(pool: &PgPool) -> Result<()> {
    for idx in [
        "idx_socio_cnpj_basico",
        "idx_socio_cpf_cnpj",
        "idx_socio_nome_trgm",
    ] {
        sqlx::query(&format!("DROP INDEX IF EXISTS {idx}"))
            .execute(pool)
            .await?;
    }
    Ok(())
}

pub async fn recreate_socio_indexes(pool: &PgPool) -> Result<()> {
    for sql in [
        "CREATE INDEX IF NOT EXISTS idx_socio_cnpj_basico ON socio (cnpj_basico)",
        "CREATE INDEX IF NOT EXISTS idx_socio_cpf_cnpj    ON socio (cnpj_cpf_socio)",
        "CREATE INDEX IF NOT EXISTS idx_socio_nome_trgm   ON socio USING GIN (nome_socio gin_trgm_ops)",
    ] {
        sqlx::query(sql).execute(pool).await?;
    }
    Ok(())
}

// ─── Sócio ───────────────────────────────────────────────────────────────────

pub async fn drop_fk_socio(pool: &PgPool) -> Result<()> {
    sqlx::query("ALTER TABLE socio DROP CONSTRAINT IF EXISTS socio_cnpj_basico_fkey")
        .execute(pool)
        .await?;
    Ok(())
}

pub async fn recreate_fk_socio(pool: &PgPool) -> Result<()> {
    sqlx::query(
        "ALTER TABLE socio \
         ADD CONSTRAINT socio_cnpj_basico_fkey \
         FOREIGN KEY (cnpj_basico) REFERENCES empresa(cnpj_basico) ON DELETE CASCADE",
    )
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn delete_orphan_socio(pool: &PgPool) -> Result<u64> {
    let res = sqlx::query(
        "DELETE FROM socio s \
         WHERE NOT EXISTS (SELECT 1 FROM empresa WHERE cnpj_basico = s.cnpj_basico)",
    )
    .execute(pool)
    .await?;
    Ok(res.rows_affected())
}

pub async fn copy_socios<R: AsyncRead + Unpin + Send + 'static>(
    pool: &PgPool,
    input: R,
) -> Result<u64> {
    let mut rdr = parse::make_deserializer(input);
    let mut rows = rdr.deserialize::<SocioRow>();

    let mut copy_in = pool
        .copy_in_raw(
            "COPY socio (cnpj_basico, identificador, nome_socio, cnpj_cpf_socio, \
             qualificacao, data_entrada, pais, cpf_repr_legal, nome_repr_legal, \
             qualif_repr_legal, faixa_etaria) \
             FROM STDIN",
        )
        .await?;

    let mut buf: Vec<u8> = Vec::with_capacity(COPY_BUF_FLUSH);
    let mut count: u64 = 0;
    while let Some(row) = rows.next().await {
        let row = row?;
        row.write_copy_line(&mut buf);
        count += 1;
        if buf.len() >= COPY_BUF_FLUSH {
            copy_in.send(buf.as_slice()).await?;
            buf.clear();
        }
    }
    if !buf.is_empty() {
        copy_in.send(buf.as_slice()).await?;
    }
    copy_in.finish().await?;
    Ok(count)
}

// ─── Simples ─────────────────────────────────────────────────────────────────

pub async fn drop_fk_simples(pool: &PgPool) -> Result<()> {
    sqlx::query("ALTER TABLE simples DROP CONSTRAINT IF EXISTS simples_cnpj_basico_fkey")
        .execute(pool)
        .await?;
    Ok(())
}

pub async fn recreate_fk_simples(pool: &PgPool) -> Result<()> {
    sqlx::query(
        "ALTER TABLE simples \
         ADD CONSTRAINT simples_cnpj_basico_fkey \
         FOREIGN KEY (cnpj_basico) REFERENCES empresa(cnpj_basico) ON DELETE CASCADE",
    )
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn delete_orphan_simples(pool: &PgPool) -> Result<u64> {
    let res = sqlx::query(
        "DELETE FROM simples s \
         WHERE NOT EXISTS (SELECT 1 FROM empresa WHERE cnpj_basico = s.cnpj_basico)",
    )
    .execute(pool)
    .await?;
    Ok(res.rows_affected())
}

pub async fn copy_simples<R: AsyncRead + Unpin + Send + 'static>(
    pool: &PgPool,
    input: R,
) -> Result<u64> {
    let mut rdr = parse::make_deserializer(input);
    let mut rows = rdr.deserialize::<SimplesRow>();

    let mut copy_in = pool
        .copy_in_raw(
            "COPY simples (cnpj_basico, opcao_simples, data_opcao_simples, data_exclusao, \
             opcao_mei, data_opcao_mei, data_exclusao_mei) FROM STDIN",
        )
        .await?;

    let mut buf: Vec<u8> = Vec::with_capacity(COPY_BUF_FLUSH);
    let mut count: u64 = 0;
    while let Some(row) = rows.next().await {
        let row = row?;
        row.write_copy_line(&mut buf);
        count += 1;
        if buf.len() >= COPY_BUF_FLUSH {
            copy_in.send(buf.as_slice()).await?;
            buf.clear();
        }
    }
    if !buf.is_empty() {
        copy_in.send(buf.as_slice()).await?;
    }
    copy_in.finish().await?;
    Ok(count)
}

// ─── Empresa ─────────────────────────────────────────────────────────────────

/// Lê rows EmpresaRow de um CSV streaming e despeja no Postgres via COPY.
pub async fn copy_empresas<R: AsyncRead + Unpin + Send + 'static>(
    pool: &PgPool,
    input: R,
) -> Result<u64> {
    let mut rdr = parse::make_deserializer(input);
    let mut rows = rdr.deserialize::<EmpresaRow>();

    let mut copy_in = pool
        .copy_in_raw(
            "COPY empresa (cnpj_basico, razao_social, natureza_juridica, \
             qualificacao_resp, capital_social, porte, ente_federativo) \
             FROM STDIN",
        )
        .await?;

    let mut buf: Vec<u8> = Vec::with_capacity(COPY_BUF_FLUSH);
    let mut count: u64 = 0;

    while let Some(row) = rows.next().await {
        let row = row?;
        row.write_copy_line(&mut buf);
        count += 1;
        if buf.len() >= COPY_BUF_FLUSH {
            copy_in.send(buf.as_slice()).await?;
            buf.clear();
        }
    }
    if !buf.is_empty() {
        copy_in.send(buf.as_slice()).await?;
    }
    copy_in.finish().await?;
    Ok(count)
}
