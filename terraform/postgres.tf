# =============================================================================
# PostgreSQL - Data Warehouse simulado (schema raw) + metadados do Airflow
# =============================================================================

resource "kubernetes_deployment_v1" "postgres" {
  metadata {
    name      = "postgres"
    namespace = kubernetes_namespace_v1.dados.metadata[0].name
    labels    = merge(local.labels, { "app.kubernetes.io/name" = "postgres" })
  }

  spec {
    replicas = 1

    # Volume RWO: o pod antigo precisa sair antes de o novo subir.
    strategy {
      type = "Recreate"
    }

    selector {
      match_labels = {
        "app.kubernetes.io/name" = "postgres"
      }
    }

    template {
      metadata {
        labels = merge(local.labels, { "app.kubernetes.io/name" = "postgres" })
      }

      spec {
        container {
          name  = "postgres"
          image = var.postgres_image

          port {
            name           = "postgres"
            container_port = 5432
          }

          # POSTGRES_USER / POSTGRES_PASSWORD / POSTGRES_DB vêm do Secret.
          env_from {
            secret_ref {
              name = kubernetes_secret_v1.postgres_superuser.metadata[0].name
            }
          }

          env {
            name  = "PGDATA"
            value = "/var/lib/postgresql/data/pgdata"
          }

          resources {
            requests = {
              cpu    = "250m"
              memory = "320Mi"
            }
            limits = {
              cpu    = "1"
              memory = "1Gi"
            }
          }

          readiness_probe {
            exec {
              command = ["/bin/sh", "-c", "pg_isready -h 127.0.0.1 -U \"$POSTGRES_USER\" -d \"$POSTGRES_DB\""]
            }
            initial_delay_seconds = 5
            period_seconds        = 5
            timeout_seconds       = 5
            failure_threshold     = 6
          }

          liveness_probe {
            exec {
              command = ["/bin/sh", "-c", "pg_isready -h 127.0.0.1 -U \"$POSTGRES_USER\" -d \"$POSTGRES_DB\""]
            }
            initial_delay_seconds = 30
            period_seconds        = 10
            timeout_seconds       = 5
            failure_threshold     = 6
          }

          volume_mount {
            name       = "dados"
            mount_path = "/var/lib/postgresql/data"
          }
        }

        volume {
          name = "dados"
          persistent_volume_claim {
            claim_name = kubernetes_persistent_volume_claim_v1.postgres_data.metadata[0].name
          }
        }
      }
    }
  }
}

# Ponto de acesso estável dentro do cluster:
# postgres.banvic-dados.svc.cluster.local:5432
resource "kubernetes_service_v1" "postgres" {
  metadata {
    name      = "postgres"
    namespace = kubernetes_namespace_v1.dados.metadata[0].name
    labels    = local.labels
  }
  spec {
    type = "ClusterIP"
    selector = {
      "app.kubernetes.io/name" = "postgres"
    }
    port {
      name        = "postgres"
      port        = local.postgres_port
      target_port = "postgres"
    }
  }
}

# Acesso a partir do seu computador (DBeaver/psql): localhost:<postgres_host_port>
resource "kubernetes_service_v1" "postgres_externo" {
  metadata {
    name      = "postgres-externo"
    namespace = kubernetes_namespace_v1.dados.metadata[0].name
    labels    = local.labels
  }
  spec {
    type = "NodePort"
    selector = {
      "app.kubernetes.io/name" = "postgres"
    }
    port {
      name        = "postgres"
      port        = local.postgres_port
      target_port = "postgres"
      node_port   = local.postgres_node_port
    }
  }
}

# -----------------------------------------------------------------------------
# Bootstrap: roles, bancos e schema raw (script SQL idempotente)
# -----------------------------------------------------------------------------
resource "kubernetes_config_map_v1" "postgres_bootstrap_sql" {
  metadata {
    name      = "postgres-bootstrap-sql"
    namespace = kubernetes_namespace_v1.dados.metadata[0].name
    labels    = local.labels
  }
  data = {
    "bootstrap_postgres.sql" = file("${path.module}/sql/bootstrap_postgres.sql")
  }
}

resource "kubernetes_job_v1" "postgres_bootstrap" {
  metadata {
    # O hash no nome faz o Job rodar de novo sempre que o SQL mudar.
    name      = "postgres-bootstrap-${substr(sha1(file("${path.module}/sql/bootstrap_postgres.sql")), 0, 8)}"
    namespace = kubernetes_namespace_v1.dados.metadata[0].name
    labels    = local.labels
  }

  spec {
    backoff_limit = 6

    template {
      metadata {
        labels = merge(local.labels, { "app.kubernetes.io/name" = "postgres-bootstrap" })
      }

      spec {
        restart_policy = "Never"

        container {
          name    = "psql"
          image   = var.postgres_image
          command = ["/bin/sh", "-c"]
          args = [
            <<-EOT
            set -eu
            until pg_isready -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d postgres; do
              echo "Aguardando o PostgreSQL..."; sleep 2
            done
            psql -d postgres \
              -v dw_database="$DW_DATABASE" \
              -v airflow_database="$AIRFLOW_DATABASE" \
              -v airflow_user="$AIRFLOW_DB_USER" -v airflow_password="$AIRFLOW_DB_PASSWORD" \
              -v loader_user="$LOADER_DB_USER"   -v loader_password="$LOADER_DB_PASSWORD" \
              -v reader_user="$READER_DB_USER"   -v reader_password="$READER_DB_PASSWORD" \
              -f /sql/bootstrap_postgres.sql
            echo "Bootstrap do PostgreSQL concluído."
            EOT
          ]

          env {
            name  = "PGHOST"
            value = local.postgres_host
          }
          env {
            name  = "PGPORT"
            value = tostring(local.postgres_port)
          }
          env {
            name = "PGUSER"
            value_from {
              secret_key_ref {
                name = kubernetes_secret_v1.postgres_superuser.metadata[0].name
                key  = "POSTGRES_USER"
              }
            }
          }
          env {
            name = "PGPASSWORD"
            value_from {
              secret_key_ref {
                name = kubernetes_secret_v1.postgres_superuser.metadata[0].name
                key  = "POSTGRES_PASSWORD"
              }
            }
          }
          env {
            name  = "DW_DATABASE"
            value = var.dw_database
          }
          env {
            name  = "AIRFLOW_DATABASE"
            value = var.airflow_database
          }

          # Usuários/senhas das roles de aplicação.
          env_from {
            secret_ref {
              name = kubernetes_secret_v1.postgres_app_roles.metadata[0].name
            }
          }

          volume_mount {
            name       = "sql"
            mount_path = "/sql"
            read_only  = true
          }
        }

        volume {
          name = "sql"
          config_map {
            name = kubernetes_config_map_v1.postgres_bootstrap_sql.metadata[0].name
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

  depends_on = [
    kubernetes_deployment_v1.postgres,
    kubernetes_service_v1.postgres,
  ]

  # Senhas trocadas -> o Job roda de novo e sincroniza as roles.
  lifecycle {
    replace_triggered_by = [
      kubernetes_secret_v1.postgres_app_roles,
      kubernetes_secret_v1.postgres_superuser,
    ]
  }
}
