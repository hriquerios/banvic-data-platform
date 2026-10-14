# Matriz de rastreabilidade — critérios da certificação × evidências

Mapa de onde cada critério do formulário de avaliação é atendido no repositório.

## Critério 1 — Infraestrutura

| Item | Evidência |
|---|---|
| 1.1.1 IaC declarativo (Terraform) | `terraform/` — um `terraform apply` cria cluster, namespaces, secrets, volumes, banco, imagens e Airflow |
| 1.1.2 Organização em arquivos | `versions.tf`, `providers.tf`, `variables.tf`, `main.tf`, `outputs.tf` + arquivos por domínio (`secrets.tf`, `storage.tf`, `postgres.tf`, `images.tf`, `airflow.tf`) |
| 1.1.3 Uso de variáveis | `terraform/variables.tf` (30 variáveis com descrição e padrão; nenhum caminho pessoal fixo) |
| 1.1.4 Idempotência | Recursos declarativos; tag das imagens = hash do conteúdo; SQL de bootstrap com `IF NOT EXISTS`; `terraform apply` repetido → `No changes` |
| 1.2.1 Imagens Docker | `docker/airflow/Dockerfile`, `docker/meltano/Dockerfile` |
| 1.2.2 Dockerfile correto | `FROM`, `ARG`, `ENV`, `WORKDIR`, `RUN`, `COPY`, `USER`, `ENTRYPOINT`; usuário não-root; checagens no build |
| 1.2.3 Volumes | PV/PVC da origem (`storage.tf`), PVC `postgres-data`, PVC `airflow-logs` (logs persistentes com retenção) |
| 1.3.1 Kubernetes local | `kind_cluster.banvic` (`main.tf`) |
| 1.3.2 Namespaces | `banvic-dados` e `banvic-orquestracao` |
| 1.3.3 Deployments | `kubernetes_deployment_v1.postgres`; Airflow api-server, scheduler e dag-processor como Deployments (`workers.persistence.enabled: false`) |
| 1.3.4 Services | `postgres` (ClusterIP), `postgres-externo` (NodePort), `airflow-api-server` (NodePort) |

## Critério 2 — Ingestão

| Item | Evidência |
|---|---|
| 2.1.1 ELT, 7 tabelas no destino bruto | `meltano/meltano.yml` → schema `raw`; transformações deixadas para camada posterior |
| 2.1.2 Full Refresh sem perda | `replication-method: FULL_TABLE` + `load_method: overwrite`; task `validar_carga` compara volume origem × destino |
| 2.1.3 Colunas e tipos íntegros | `stream_maps` tipam cada coluna; `validar_carga` confere o tipo de cada coluna em `information_schema` (README §6) |
| 2.1.4 Credenciais fora do meltano.yml | Nenhum dado de conexão no arquivo: variáveis `TARGET_POSTGRES_*` (setting `password` com `sensitive: true`); `.env` local ignorado; Secret no Kubernetes |
| 2.2.1 Tap/Target modulares | `tap-csv` + `target-postgres` declarados no `meltano.yml` (namespace, executável, settings) e 7 extractors herdados `tap-banvic-<tabela>` |
| 2.2.2 `meltano run` | `meltano run tap-banvic-<tabela> target-postgres` (task `carregar_raw`) |
| 2.2.3 `meltano select` | Bloco `select` com todas as streams e atributos explícitos + `select_filter` por extractor |

## Critério 3 — Airflow

| Item | Evidência (`dags/banvic_erp_ingestao_raw.py`) |
|---|---|
| 3.1.1 dag_id, start_date, schedule | `banvic_erp_ingestao_raw`, `2026-01-01`, `0 6 * * *` |
| 3.1.2 catchup | `catchup=False` |
| 3.1.3 default_args | retries, backoff, timeout e callbacks |
| 3.1.4 Documentação | `description`, `doc_md`, `tags`, tooltips |
| 3.2.1 Operadores | `FileSensor`, `KubernetesPodOperator`, `EmptyOperator`, TaskFlow `@task`, `PostgresHook` |
| 3.2.2 Sensores | `aguardar_arquivo` por tabela (modo `reschedule`) |
| 3.2.3 Dependências | `chain()` e TaskGroups |
| 3.2.4 Atomicidade | Uma operação por task; uma TaskGroup independente por tabela |
| 3.3.1 Sem top-level pesado | I/O, hooks e consultas apenas dentro das tasks |
| 3.3.2 Variables via Jinja | `{{ var.value.banvic_* }}` |
| 3.3.3 XCom pequeno | Apenas `{tabela, linhas, sha256, bytes}` |

## Critério 4 — Segurança

| Item | Evidência |
|---|---|
| 4.1.1 Sem hardcode | Nenhuma senha no repositório (teste `test_sem_credenciais_no_codigo`) |
| 4.1.2 `sensitive = true` | 5 variáveis de senha + outputs sensíveis |
| 4.2.1 Kubernetes Secret | `terraform/secrets.tf` |
| 4.2.2 Menor privilégio | Secret de escrita só nos pods do Meltano; Connection só no scheduler; roles `loader`/`reader`/`airflow_meta`; SA sem token |
| 4.2.3 `.env` + `.gitignore` | `meltano/.env.example`; `.gitignore` cobre `.env`, `*.tfvars`, `*.tfstate*` |
| 4.3.1 Connections | `AIRFLOW_CONN_BANVIC_DW` (Secret → env do scheduler) |
| 4.3.2 Nomes mascarados | Senhas em campos `password` de Connection e variáveis `*_PASSWORD` |

## Critério 5 — Documentação

| Item | Evidência |
|---|---|
| 5.1.1 Arquitetura e escolhas | `README.md` §2 e §3, `docs/arquitetura.png` |
| 5.1.2 Passo a passo reproduzível | `README.md` §5 e `scripts/` |
