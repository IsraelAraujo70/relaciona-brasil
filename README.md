# Relaciona Brasil

API HTTP open-source que expõe os relacionamentos entre empresas e pessoas no Brasil — sócios, holdings, vínculos cruzados — a partir do dump público de CNPJs da Receita Federal.

**Por quê?** Pra qualquer pessoa poder investigar a quem pertencem as empresas brasileiras, identificar sócios em comum, holdings de fachada e padrões consistentes com lavagem de dinheiro. Transparência cívica como serviço público.

**Em produção:** [relaciona.israeldeveloper.com.br](https://relaciona.israeldeveloper.com.br) · Swagger UI em [`/docs`](https://relaciona.israeldeveloper.com.br/docs).

## Como funciona

A versão original tentava ingerir os ~60 milhões de linhas do dump num Postgres mensal — inviável no hardware disponível (HDD lento, 95% iowait sob load). O modelo atual é **lazy lookup com cache**:

1. **Downloader** (cron mensal, dia 15 às 06:00 UTC) baixa os 31 zips da vintage corrente da Receita pra um volume em disco (~7,5 GB por vintage). Mantém só as 2 mais recentes.
2. **API** atende `/v1/empresas/:cnpj` e `/v1/relacionamento/:cnpj` checando o cache do Postgres primeiro.
   - **Cache hit** (200) → resposta imediata, < 1 s.
   - **Cache miss** (202) → enfileira um job e devolve `{ job_id, poll_url }`.
3. **Worker** consome a fila: varre os 31 zips em paralelo filtrando pelo CNPJ-alvo (~14 min em HDD frio), persiste o que achou no Postgres como cache, marca o job concluído e dispara webhook se solicitado.
4. **Cliente**: faz polling em `/v1/jobs/:id` ou recebe POST no `?callback=URL`.

Resultado: Postgres pequeno (cresce com uso), disco constante, primeira leitura cara, leituras subsequentes rápidas.

## Endpoints

| Método | Rota | Códigos | Descrição |
|---|---|---|---|
| `GET` | `/health` | 200 | Liveness |
| `GET` | `/ready` | 200 / 503 | Pronto se tem ao menos uma vintage baixada |
| `GET` | `/v1/empresas/:cnpj` | 200 / 202 / 400 | Empresa + estabelecimentos + sócios |
| `GET` | `/v1/relacionamento/:doc?profundidade=N&callback=URL` | 200 / 202 / 400 | Grafo de relacionamentos (1-4 hops, default 2) |
| `GET` | `/v1/socios?nome=…` ou `?cpf=…` | 200 / 400 | Busca de sócios **só no cache** (trigram para nome) |
| `GET` | `/v1/jobs/:id` | 200 / 404 | Status de um job assíncrono |

CPF input em `/v1/relacionamento/` retorna **só** o que está no cache — não há reverse lookup nos zips porque eles são indexados por `cnpj_basico`.

## Stack

Rust 1.85+ (axum + tokio) · PostgreSQL 16 · Kamal 2 · GitHub Actions · Cloudflare Tunnel

## Desenvolvimento local

Pré-requisitos: Rust stable (≥ 1.85), Docker.

```bash
cp .env.example .env
docker compose up -d postgres
cargo run -- --mode migrate

# Em terminais separados:
cargo run -- --mode downloader --once  # baixa a vintage atual (~10 min, ~7,5 GB)
cargo run -- --mode worker              # consome a fila
cargo run -- --mode api                 # serve a HTTP

# Smoke:
curl 'http://localhost:8080/v1/empresas/00000000000191'   # 202 com job_id
curl 'http://localhost:8080/v1/jobs/<id>'                  # poll até completed
curl 'http://localhost:8080/v1/empresas/00000000000191'   # 200 cache hit
```

## Modos de execução (`--mode`)

| Modo | Descrição |
|---|---|
| `api` | Serve HTTP em `$PORT` (default 8080) |
| `worker` | Consome a fila de jobs |
| `downloader` | Roda o cron mensal (`--once` baixa e sai) |
| `migrate` | Aplica as migrations e sai |

## Configuração (env vars)

| Var | Default | Notas |
|---|---|---|
| `DATABASE_URL` | — | Postgres com `?sslmode=disable` em local |
| `DUMP_BASE_URL` | — | URL do share Nextcloud da Receita |
| `DUMP_DATA_DIR` | `/app/data/dumps` | Onde os zips baixados ficam |
| `PORT` | `8080` | Porta HTTP |
| `SCAN_PARALLELISM` | `4` | Tasks paralelas do scan |
| `KEEP_VINTAGES` | `2` | Quantas vintages manter em disco |
| `RUST_LOG` | — | Filtro de logs |

## Webhook

Em `/v1/relacionamento/:doc?callback=https://…`, quando o worker concluir, faz `POST` com:

```json
{ "job_id": "...", "status": "completed", "resultado": {...}, "erro": null }
```

3 retries com backoff (5 s, 30 s, 2 min). Falha de delivery não invalida o job.

## Fonte de dados

[Dados Abertos do CNPJ — Receita Federal](https://arquivos.receitafederal.gov.br/index.php/s/YggdBLfdninEJX9). A Receita publica entre os dias 10 e 12 de cada mês; o downloader roda no dia 15.

CPFs vêm mascarados (`***NNNNNN**`) na origem — não há como reverter, e respeitamos isso por princípio (LGPD).

## Licença

[AGPL-3.0-or-later](LICENSE). Forks e qualquer serviço público em cima desse código devem continuar open-source — incluindo modificações servidas via rede.
