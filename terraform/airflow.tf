# =============================================================================
# Apache Airflow (Helm chart oficial) + recursos de apoio da ingestão
# =============================================================================

# ServiceAccount dos pods de ingestão (Meltano): sem permissões no cluster e
# sem token montado. O pod só precisa ler os CSVs e falar com o PostgreSQL.
resource "kubernetes_service_account_v1" "ingestao" {
  metadata {
    name      = "banvic-ingestao"
    namespace = kubernetes_namespace_v1.orquestracao.metadata[0].name
    labels    = local.labels
  }
  automount_service_account_token = false
}

resource "helm_release" "airflow" {
  name       = "airflow"
  repository = "https://airflow.apache.org"
  chart      = "airflow"
  version    = var.airflow_chart_version
  namespace  = kubernetes_namespace_v1.orquestracao.metadata[0].name

  wait            = true
  timeout         = 900
  cleanup_on_fail = true

  values = [
    templatefile("${path.module}/templates/airflow-values.yaml.tftpl", {
      airflow_version          = var.airflow_version
      airflow_image_repository = var.airflow_image_repository
      airflow_image_tag        = local.airflow_image_tag
      meltano_image            = local.meltano_image
      namespace                = kubernetes_namespace_v1.orquestracao.metadata[0].name
      fernet_secret            = kubernetes_secret_v1.airflow_fernet_key.metadata[0].name
      api_secret               = kubernetes_secret_v1.airflow_api_secret_key.metadata[0].name
      jwt_secret               = kubernetes_secret_v1.airflow_jwt_secret.metadata[0].name
      metadata_secret          = kubernetes_secret_v1.airflow_metadata.metadata[0].name
      connections_secret       = kubernetes_secret_v1.airflow_connections.metadata[0].name
      logs_pvc                 = kubernetes_persistent_volume_claim_v1.airflow_logs.metadata[0].name
      source_pvc               = kubernetes_persistent_volume_claim_v1.fonte_erp.metadata[0].name
      source_mount_path        = local.source_mount_path
      airflow_node_port        = local.airflow_node_port
      parallelism              = var.airflow_parallelism
      log_retention_days       = var.airflow_log_retention_days
    })
  ]

  depends_on = [
    terraform_data.airflow_image,
    terraform_data.meltano_image,
    kubernetes_job_v1.postgres_bootstrap,
    kubernetes_secret_v1.loader_credentials,
    kubernetes_service_account_v1.ingestao,
  ]
}

# Cria (uma única vez) o usuário admin da UI. A senha vem do Secret
# airflow-admin e nunca aparece em values, logs ou argumentos de comando.
resource "kubernetes_job_v1" "airflow_admin_user" {
  metadata {
    name      = "airflow-criar-admin"
    namespace = kubernetes_namespace_v1.orquestracao.metadata[0].name
    labels    = local.labels
  }

  spec {
    backoff_limit = 4

    template {
      metadata {
        labels = merge(local.labels, { "app.kubernetes.io/name" = "airflow-criar-admin" })
      }

      spec {
        restart_policy = "Never"

        security_context {
          run_as_user     = "50000"
          run_as_group    = "0"
          run_as_non_root = true
        }

        container {
          name    = "criar-admin"
          image   = local.airflow_image
          command = ["bash", "-c"]
          args = [
            <<-EOT
            set -euo pipefail
            # "users create" é idempotente: se o usuário já existir, o FAB
            # apenas informa e encerra com sucesso.
            airflow users create \
              --username "$AIRFLOW_ADMIN_USERNAME" \
              --password "$AIRFLOW_ADMIN_PASSWORD" \
              --email "$AIRFLOW_ADMIN_EMAIL" \
              --firstname Administrador --lastname Plataforma --role Admin
            EOT
          ]

          env {
            name  = "AIRFLOW__CORE__AUTH_MANAGER"
            value = "airflow.providers.fab.auth_manager.fab_auth_manager.FabAuthManager"
          }
          env {
            name = "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN"
            value_from {
              secret_key_ref {
                name = kubernetes_secret_v1.airflow_metadata.metadata[0].name
                key  = "connection"
              }
            }
          }
          env_from {
            secret_ref {
              name = kubernetes_secret_v1.airflow_admin.metadata[0].name
            }
          }
        }
      }
    }
  }

  wait_for_completion = true

  timeouts {
    create = "5m"
    update = "5m"
  }

  depends_on = [helm_release.airflow]

  lifecycle {
    replace_triggered_by = [kubernetes_secret_v1.airflow_admin]
  }
}
