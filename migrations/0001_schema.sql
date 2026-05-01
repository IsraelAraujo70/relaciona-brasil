-- Relaciona Brasil — schema inicial
-- Baseado no Layout dos Dados Abertos do CNPJ (Receita Federal).
-- Convenção: tabelas e colunas em português, code-points exatos do dump quando aplicável.

CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- ──────────────────────────────────────────────────────────────────────────────
-- Tabelas de lookup (poucos MBs cada, populadas a cada vintage)
-- ──────────────────────────────────────────────────────────────────────────────

CREATE TABLE cnae (
    codigo    CHAR(7)  PRIMARY KEY,
    descricao TEXT     NOT NULL
);

CREATE TABLE municipio (
    codigo    CHAR(4)  PRIMARY KEY,
    descricao TEXT     NOT NULL
);

CREATE TABLE pais (
    codigo    CHAR(3)  PRIMARY KEY,
    descricao TEXT     NOT NULL
);

CREATE TABLE natureza (
    codigo    CHAR(4)  PRIMARY KEY,
    descricao TEXT     NOT NULL
);

CREATE TABLE qualificacao (
    codigo    SMALLINT PRIMARY KEY,
    descricao TEXT     NOT NULL
);

CREATE TABLE motivo (
    codigo    SMALLINT PRIMARY KEY,
    descricao TEXT     NOT NULL
);

-- ──────────────────────────────────────────────────────────────────────────────
-- Núcleo: empresas e estabelecimentos (filtro: apenas situação cadastral '02')
-- ──────────────────────────────────────────────────────────────────────────────

-- Nota: as FKs para tabelas de lookup (natureza, qualificacao, cnae, motivo,
-- pais, municipio) foram removidas porque o dump da Receita ocasionalmente
-- referencia códigos ausentes do próprio dump. Mantemos apenas as FKs
-- internas estabelecimento/sócio/simples → empresa, que são consistentes.

CREATE TABLE empresa (
    cnpj_basico       CHAR(8)        PRIMARY KEY,
    razao_social      TEXT,
    natureza_juridica CHAR(4),
    qualificacao_resp SMALLINT,
    capital_social    NUMERIC(20, 2),
    porte             CHAR(2),
    ente_federativo   TEXT
);

CREATE TABLE estabelecimento (
    cnpj_basico       CHAR(8)  NOT NULL REFERENCES empresa,
    cnpj_ordem        CHAR(4)  NOT NULL,
    cnpj_dv           CHAR(2)  NOT NULL,
    cnpj              CHAR(14) GENERATED ALWAYS AS (cnpj_basico || cnpj_ordem || cnpj_dv) STORED,
    matriz_filial     CHAR(1),
    nome_fantasia     TEXT,
    situacao          CHAR(2),
    data_situacao     DATE,
    motivo_situacao   SMALLINT,
    nome_cidade_ext   TEXT,
    pais              CHAR(3),
    data_inicio       DATE,
    cnae_principal    CHAR(7),
    cnaes_secundarios CHAR(7)[],
    tipo_logradouro   TEXT,
    logradouro        TEXT,
    numero            TEXT,
    complemento       TEXT,
    bairro            TEXT,
    cep               CHAR(8),
    uf                CHAR(2),
    municipio         CHAR(4),
    ddd_1             TEXT,
    telefone_1        TEXT,
    ddd_2             TEXT,
    telefone_2        TEXT,
    ddd_fax           TEXT,
    fax               TEXT,
    email             TEXT,
    situacao_especial TEXT,
    data_sit_especial DATE,
    PRIMARY KEY (cnpj_basico, cnpj_ordem, cnpj_dv)
);

CREATE INDEX idx_estab_cnpj      ON estabelecimento (cnpj);
CREATE INDEX idx_estab_municipio ON estabelecimento (municipio);
CREATE INDEX idx_estab_cnae      ON estabelecimento (cnae_principal);
CREATE INDEX idx_estab_uf        ON estabelecimento (uf);

-- ──────────────────────────────────────────────────────────────────────────────
-- Sócios — coração do projeto
-- ──────────────────────────────────────────────────────────────────────────────

CREATE TABLE socio (
    cnpj_basico       CHAR(8)     NOT NULL REFERENCES empresa,
    identificador     SMALLINT    NOT NULL,            -- 1=PJ, 2=PF, 3=Estrangeira
    nome_socio        TEXT,
    cnpj_cpf_socio    VARCHAR(14),                     -- CPF mascarado (***NNNNNN**) ou CNPJ
    qualificacao      SMALLINT,
    data_entrada      DATE,
    pais              CHAR(3),
    cpf_repr_legal    VARCHAR(14),
    nome_repr_legal   TEXT,
    qualif_repr_legal SMALLINT,
    faixa_etaria      SMALLINT
);

CREATE INDEX idx_socio_cnpj_basico ON socio (cnpj_basico);
CREATE INDEX idx_socio_cpf_cnpj    ON socio (cnpj_cpf_socio);
CREATE INDEX idx_socio_nome_trgm   ON socio USING GIN (nome_socio gin_trgm_ops);

-- ──────────────────────────────────────────────────────────────────────────────
-- Simples / MEI
-- ──────────────────────────────────────────────────────────────────────────────

CREATE TABLE simples (
    cnpj_basico        CHAR(8) PRIMARY KEY REFERENCES empresa,
    opcao_simples      BOOLEAN,
    data_opcao_simples DATE,
    data_exclusao      DATE,
    opcao_mei          BOOLEAN,
    data_opcao_mei     DATE,
    data_exclusao_mei  DATE
);

-- ──────────────────────────────────────────────────────────────────────────────
-- Controle de ingestão
-- ──────────────────────────────────────────────────────────────────────────────

CREATE TABLE ingestao_status (
    vintage         CHAR(7)     PRIMARY KEY,           -- "2026-04"
    iniciada_em     TIMESTAMPTZ NOT NULL DEFAULT now(),
    terminada_em    TIMESTAMPTZ,
    etapa           TEXT        NOT NULL,              -- lookups | empresas | estabelecimentos | socios | simples | concluida
    bytes_baixados  BIGINT,
    linhas_inseridas BIGINT,
    erro            TEXT
);

CREATE INDEX idx_status_terminada ON ingestao_status (terminada_em DESC NULLS LAST);
