-- Pivot para modelo lazy/cached:
--   * Tabelas existentes (empresa/estabelecimento/socio/simples/lookups) agora
--     funcionam como CACHE alimentado por consultas sob demanda.
--   * `ingestao_status` (que controlava o pipeline bulk) é descartada.
--   * `vintages_baixadas` registra quais dumps estão prontos no disco local.
--   * `jobs` é a fila de consultas assíncronas — workers consomem e gravam o
--     resultado serializado em `resultado` para entrega via polling/webhook.

DROP TABLE IF EXISTS ingestao_status;

ALTER TABLE empresa
    ADD COLUMN IF NOT EXISTS consultado_em TIMESTAMPTZ NOT NULL DEFAULT now(),
    ADD COLUMN IF NOT EXISTS vintage       CHAR(7);

ALTER TABLE estabelecimento
    ADD COLUMN IF NOT EXISTS consultado_em TIMESTAMPTZ NOT NULL DEFAULT now(),
    ADD COLUMN IF NOT EXISTS vintage       CHAR(7);

ALTER TABLE socio
    ADD COLUMN IF NOT EXISTS consultado_em TIMESTAMPTZ NOT NULL DEFAULT now(),
    ADD COLUMN IF NOT EXISTS vintage       CHAR(7);

ALTER TABLE simples
    ADD COLUMN IF NOT EXISTS consultado_em TIMESTAMPTZ NOT NULL DEFAULT now(),
    ADD COLUMN IF NOT EXISTS vintage       CHAR(7);

CREATE TABLE IF NOT EXISTS vintages_baixadas (
    vintage     CHAR(7)     PRIMARY KEY,                -- ex.: "2026-04"
    baixada_em  TIMESTAMPTZ NOT NULL DEFAULT now(),
    bytes       BIGINT,
    caminho     TEXT        NOT NULL                    -- /app/data/dumps/2026-04
);

CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE IF NOT EXISTS jobs (
    id              UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    cnpj_basico     CHAR(8)      NOT NULL,
    profundidade    SMALLINT     NOT NULL DEFAULT 1,
    callback_url    TEXT,
    status          TEXT         NOT NULL DEFAULT 'pending',
                                              -- pending | running | completed | failed
    criado_em       TIMESTAMPTZ  NOT NULL DEFAULT now(),
    iniciado_em     TIMESTAMPTZ,
    finalizado_em   TIMESTAMPTZ,
    erro            TEXT,
    resultado       JSONB
);

CREATE INDEX IF NOT EXISTS idx_jobs_pending
    ON jobs (criado_em)
    WHERE status = 'pending';

-- Dedup: 1 job in-flight por (cnpj_basico, profundidade). Múltiplas requisições
-- pra mesmo alvo viram poll do mesmo job.
CREATE UNIQUE INDEX IF NOT EXISTS idx_jobs_inflight
    ON jobs (cnpj_basico, profundidade)
    WHERE status IN ('pending', 'running');
