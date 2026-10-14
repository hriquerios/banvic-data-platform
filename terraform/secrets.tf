# =============================================================================
# Credenciais
#
# Nenhuma senha é escrita no repositório. Elas são (a) informadas por
# variáveis sensitive ou (b) geradas aqui com random_password, e então
# gravadas em Secrets Kubernetes. Cada Secret contém apenas o que o seu
# consumidor precisa (princípio do menor privilégio):
#
#   Secret                       Namespace       Consumidor
#   ---------------------------  --------------  ------------------------------
#   postgres-superuser           banvic-dados    Deployment do PostgreSQL, Job bootstrap
#   postgres-app-roles           banvic-dados    Job bootstrap (cria/sincroniza roles)
#   airflow-metadata             orquestracao    Componentes do Airflow (banco de metadados)
#   airflow-fernet-key/api/jwt   orquestracao    Componentes do Airflow
#   airflow-admin                orquestracao    Job que cria o usuário admin da UI
#   banvic-airflow-connections   orquestracao    SOMENTE o scheduler (Connection banvic_dw, leitura)
#   banvic-loader-credentials    orquestracao    SOMENTE os pods de ingestão (Meltano, escrita)
#
# Observação: o state do Terraform guarda esses valores; por isso
# terraform.tfstate* está no .gitignore.
# =============================================================================

resource "random_password" "postgres_admin" {
  length  = 32
  special = false
}

resource "random_password" "airflow_db" {
  length  = 32
  special = false
}

resource "random_password" "loader_db" {
  length  = 32
  special = false
}

resource "random_password" "reader_db" {
  length  = 32
  special = false
}

resource "random_password" "airflow_admin" {
  length  = 20
  special = false
}

resource "random_id" "fernet_key" {
  byte_length = 32
}

resource "random_password" "airflow_api_secret_key" {
  length  = 48
  special = false
}

resource "random_password" "airflow_jwt_secret" {
  length  = 48
  special = false
}

locals {
  postgres_admin_password = coalesce(var.postgres_admin_password, random_password.postgres_admin.result)
  airflow_db_password     = coalesce(var.airflow_db_password, random_password.airflow_db.result)
  loader_db_password      = coalesce(var.loader_db_password, random_password.loader_db.result)
  reader_db_password      = coalesce(var.reader_db_password, random_password.reader_db.result)
  airflow_admin_password  = coalesce(var.airflow_admin_password, random_password.airflow_admin.result)
}

# -----------------------------------------------------------------------------
# Namespace banvic-dados
# -----------------------------------------------------------------------------
resource "kubernetes_secret_v1" "postgres_superuser" {
  metadata {
    name      = "postgres-superuser"
    namespace = kubernetes_namespace_v1.dados.metadata[0].name
    labels    = local.labels
  }
  type = "Opaque"
  data = {
    POSTGRES_USER     = var.postgres_admin_user
    POSTGRES_PASSWORD = local.postgres_admin_password
    POSTGRES_DB       = var.dw_database
  }
}

resource "kubernetes_secret_v1" "postgres_app_roles" {
  metadata {
    name      = "postgres-app-roles"
    namespace = kubernetes_namespace_v1.dados.metadata[0].name
    labels    = local.labels
  }
  type = "Opaque"
  data = {
    AIRFLOW_DB_USER     = var.airflow_db_user
    AIRFLOW_DB_PASSWORD = local.airflow_db_password
    LOADER_DB_USER      = var.loader_db_user
    LOADER_DB_PASSWORD  = local.loader_db_password
    READER_DB_USER      = var.reader_db_user
    READER_DB_PASSWORD  = local.reader_db_password
  }
}

# -----------------------------------------------------------------------------
# Namespace banvic-orquestracao
# -----------------------------------------------------------------------------
resource "kubernetes_secret_v1" "airflow_metadata" {
  metadata {
    name      = "airflow-metadata"
    namespace = kubernetes_namespace_v1.orquestracao.metadata[0].name
    labels    = local.labels
  }
  type = "Opaque"
  data = {
    connection = "postgresql://${var.airflow_db_user}:${local.airflow_db_password}@${local.postgres_host}:${local.postgres_port}/${var.airflow_database}"
  }
}

resource "kubernetes_secret_v1" "airflow_fernet_key" {
  metadata {
    name      = "airflow-fernet-key"
    namespace = kubernetes_namespace_v1.orquestracao.metadata[0].name
    labels    = local.labels
  }
  type = "Opaque"
  data = {
    # Fernet exige 32 bytes em base64 url-safe com padding.
    fernet-key = "${random_id.fernet_key.b64_url}="
  }
}

resource "kubernetes_secret_v1" "airflow_api_secret_key" {
  metadata {
    name      = "airflow-api-secret-key"
    namespace = kubernetes_namespace_v1.orquestracao.metadata[0].name
    labels    = local.labels
  }
  type = "Opaque"
  data = {
    api-secret-key = random_password.airflow_api_secret_key.result
  }
}

resource "kubernetes_secret_v1" "airflow_jwt_secret" {
  metadata {
    name      = "airflow-jwt-secret"
    namespace = kubernetes_namespace_v1.orquestracao.metadata[0].name
    labels    = local.labels
  }
  type = "Opaque"
  data = {
    jwt-secret = random_password.airflow_jwt_secret.result
  }
}

resource "kubernetes_secret_v1" "airflow_admin" {
  metadata {
    name      = "airflow-admin"
    namespace = kubernetes_namespace_v1.orquestracao.metadata[0].name
    labels    = local.labels
  }
  type = "Opaque"
  data = {
    AIRFLOW_ADMIN_USERNAME = var.airflow_admin_user
    AIRFLOW_ADMIN_PASSWORD = local.airflow_admin_password
    AIRFLOW_ADMIN_EMAIL    = var.airflow_admin_email
  }
}

# Connection do Airflow no padrão AIRFLOW_CONN_<CONN_ID> (formato JSON).
# Usuário SOMENTE LEITURA: as tasks de validação não conseguem alterar dados.
resource "kubernetes_secret_v1" "airflow_connections" {
  metadata {
    name      = "banvic-airflow-connections"
    namespace = kubernetes_namespace_v1.orquestracao.metadata[0].name
    labels    = local.labels
  }
  type = "Opaque"
  data = {
    AIRFLOW_CONN_BANVIC_DW = jsonencode({
      conn_type = "postgres"
      host      = local.postgres_host
      port      = local.postgres_port
      schema    = var.dw_database
      login     = var.reader_db_user
      password  = local.reader_db_password
    })
  }
}

# Variáveis lidas pelo target-postgres do Meltano. Injetado via envFrom
# apenas nos pods de ingestão criados pelo KubernetesPodOperator.
resource "kubernetes_secret_v1" "loader_credentials" {
  metadata {
    name      = "banvic-loader-credentials"
    namespace = kubernetes_namespace_v1.orquestracao.metadata[0].name
    labels    = local.labels
  }
  type = "Opaque"
  data = {
    TARGET_POSTGRES_HOST     = local.postgres_host
    TARGET_POSTGRES_PORT     = tostring(local.postgres_port)
    TARGET_POSTGRES_DATABASE = var.dw_database
    TARGET_POSTGRES_USER     = var.loader_db_user
    TARGET_POSTGRES_PASSWORD = local.loader_db_password
  }
}
