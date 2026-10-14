-- =============================================================================
-- Bootstrap idempotente do PostgreSQL do BanVic
--
-- Executado pelo Job Kubernetes "postgres-bootstrap" (criado pelo Terraform)
-- a cada `terraform apply` em que algo relevante mudar. Pode ser reexecutado
-- quantas vezes for necessário: nada é duplicado e as senhas são apenas
-- sincronizadas com os Secrets.
--
-- Variáveis psql (passadas com -v a partir de variáveis de ambiente do Job,
-- que por sua vez vêm de Secrets Kubernetes - nada fica escrito aqui):
--   dw_database, airflow_database
--   airflow_user, airflow_password
--   loader_user,  loader_password
--   reader_user,  reader_password
--
-- Papéis criados (princípio do menor privilégio):
--   airflow_user -> dono apenas do banco de metadados do Airflow
--   loader_user  -> dono do schema raw (cria/substitui tabelas) - usado só
--                   pelos pods de ingestão do Meltano
--   reader_user  -> somente leitura no schema raw - usado pela Connection
--                   do Airflow nas tasks de validação
-- =============================================================================

\set ON_ERROR_STOP on

-- ----------------------------------------------------------------------------
-- Roles
-- ----------------------------------------------------------------------------
SELECT format('CREATE ROLE %I LOGIN', :'airflow_user')
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = :'airflow_user') \gexec

SELECT format('CREATE ROLE %I LOGIN', :'loader_user')
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = :'loader_user') \gexec

SELECT format('CREATE ROLE %I LOGIN', :'reader_user')
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = :'reader_user') \gexec

-- Mantém as senhas sempre sincronizadas com os Secrets.
ALTER ROLE :"airflow_user" WITH LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE PASSWORD :'airflow_password';
ALTER ROLE :"loader_user"  WITH LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE PASSWORD :'loader_password';
ALTER ROLE :"reader_user"  WITH LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE PASSWORD :'reader_password';

-- ----------------------------------------------------------------------------
-- Bancos
-- ----------------------------------------------------------------------------
SELECT format('CREATE DATABASE %I OWNER %I', :'airflow_database', :'airflow_user')
WHERE NOT EXISTS (SELECT 1 FROM pg_database WHERE datname = :'airflow_database') \gexec

SELECT format('CREATE DATABASE %I', :'dw_database')
WHERE NOT EXISTS (SELECT 1 FROM pg_database WHERE datname = :'dw_database') \gexec

ALTER DATABASE :"airflow_database" OWNER TO :"airflow_user";

-- Ninguém conecta nos bancos sem permissão explícita.
REVOKE CONNECT, TEMPORARY ON DATABASE :"airflow_database" FROM PUBLIC;
REVOKE CONNECT, TEMPORARY ON DATABASE :"dw_database" FROM PUBLIC;
GRANT CONNECT ON DATABASE :"airflow_database" TO :"airflow_user";
GRANT CONNECT, TEMPORARY ON DATABASE :"dw_database" TO :"loader_user";
GRANT CONNECT ON DATABASE :"dw_database" TO :"reader_user";

-- ----------------------------------------------------------------------------
-- Schema raw (camada de dados brutos do ELT)
-- ----------------------------------------------------------------------------
\connect :"dw_database"

REVOKE ALL ON SCHEMA public FROM PUBLIC;

CREATE SCHEMA IF NOT EXISTS raw AUTHORIZATION :"loader_user";
ALTER SCHEMA raw OWNER TO :"loader_user";
COMMENT ON SCHEMA raw IS 'Camada RAW do BanVic: cópia fiel (tipada) das 7 tabelas do ERP, carregada pelo Meltano.';

GRANT USAGE ON SCHEMA raw TO :"reader_user";
GRANT SELECT ON ALL TABLES IN SCHEMA raw TO :"reader_user";

-- Tabelas que o loader criar no futuro (a carga overwrite recria as tabelas)
-- já nascem legíveis pelo usuário de leitura.
ALTER DEFAULT PRIVILEGES FOR ROLE :"loader_user" IN SCHEMA raw
    GRANT SELECT ON TABLES TO :"reader_user";
