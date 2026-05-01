# Relaciona Brasil

API HTTP open-source que ingere mensalmente o dump público de CNPJs da Receita Federal e expõe os relacionamentos entre empresas e pessoas como JSON.

**Por quê?** Pra qualquer pessoa poder investigar a quem pertencem as empresas brasileiras, identificar sócios em comum, holdings de fachada e padrões consistentes com lavagem de dinheiro. Transparência cívica como serviço público.

## Status

Em construção. Roadmap:

- [ ] **Fase 1** — Ingestão mensal funcionando
- [ ] **Fase 2** — API de relacionamento
- [ ] **Fase 3** — Polimento, observabilidade, contribuições

## Stack

Rust (axum + tokio) · PostgreSQL · Kamal 2 · GitHub Actions

## Endpoints (planejados)

| Método | Rota | Descrição |
|---|---|---|
| `GET` | `/health` | Liveness |
| `GET` | `/ready` | Readiness — status da última ingestão |
| `GET` | `/v1/empresas/:cnpj` | Empresa + estabelecimentos + sócios |
| `GET` | `/v1/socios?nome=…` | Busca por sócios |
| `GET` | `/v1/relacionamento/:doc?profundidade=N` | Grafo de relacionamentos a partir de um CPF/CNPJ |

## Desenvolvimento local

Pré-requisitos: Rust stable (≥1.83), Docker, sqlx-cli (`cargo install sqlx-cli --no-default-features --features postgres`).

```bash
cp .env.example .env
docker compose up -d postgres
cargo run -- --mode migrate
cargo run -- --mode api &
curl localhost:8080/health
```

Pra disparar uma ingestão manual contra um dump reduzido:

```bash
DUMP_BASE_URL=<url> cargo run -- --mode worker --once
```

## Fonte de dados

[Dados Abertos do CNPJ — Receita Federal](https://arquivos.receitafederal.gov.br/index.php/s/YggdBLfdninEJX9). A Receita publica entre os dias 10 e 12 de cada mês; o job interno roda no dia 15.

CPFs vêm mascarados (`***NNNNNN**`) na origem — não há como reverter, e respeitamos isso por princípio (LGPD).

## Licença

[AGPL-3.0-or-later](LICENSE). Forks e qualquer serviço público em cima desse código devem continuar open-source — incluindo modificações servidas via rede.
