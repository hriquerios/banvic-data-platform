# =============================================================================
# Volumes
# =============================================================================

# Origem dos dados: a pasta data/banvic_erp do host, montada no nó Kind
# (extra_mounts), exposta como PersistentVolume estático somente leitura.
# Os CSVs NÃO ficam dentro de imagens: basta trocar os arquivos na pasta.
resource "kubernetes_persistent_volume_v1" "fonte_erp" {
  metadata {
    name   = "banvic-fonte-erp"
    labels = local.labels
  }
  spec {
    capacity = {
      storage = "1Gi"
    }
    access_modes                     = ["ReadOnlyMany"]
    persistent_volume_reclaim_policy = "Retain"
    storage_class_name               = "banvic-estatico"

    persistent_volume_source {
      host_path {
        path = var.node_source_path
        type = "Directory"
      }
    }

    claim_ref {
      namespace = kubernetes_namespace_v1.orquestracao.metadata[0].name
      name      = "banvic-fonte-erp"
    }
  }
}

resource "kubernetes_persistent_volume_claim_v1" "fonte_erp" {
  metadata {
    name      = "banvic-fonte-erp"
    namespace = kubernetes_namespace_v1.orquestracao.metadata[0].name
    labels    = local.labels
  }
  spec {
    access_modes       = ["ReadOnlyMany"]
    storage_class_name = "banvic-estatico"
    volume_name        = kubernetes_persistent_volume_v1.fonte_erp.metadata[0].name
    resources {
      requests = {
        storage = "1Gi"
      }
    }
  }
  wait_until_bound = true
}

# Logs das tasks do Airflow: sobrevivem a reinícios/recriação dos pods.
# StorageClass "standard" = local-path-provisioner que já vem no Kind.
resource "kubernetes_persistent_volume_claim_v1" "airflow_logs" {
  metadata {
    name      = "airflow-logs"
    namespace = kubernetes_namespace_v1.orquestracao.metadata[0].name
    labels    = local.labels
  }
  spec {
    access_modes       = ["ReadWriteOnce"]
    storage_class_name = "standard"
    resources {
      requests = {
        storage = var.airflow_logs_storage_size
      }
    }
  }
  # local-path usa WaitForFirstConsumer: o bind só ocorre quando um pod usar o PVC.
  wait_until_bound = false
}

# Dados do PostgreSQL.
resource "kubernetes_persistent_volume_claim_v1" "postgres_data" {
  metadata {
    name      = "postgres-data"
    namespace = kubernetes_namespace_v1.dados.metadata[0].name
    labels    = local.labels
  }
  spec {
    access_modes       = ["ReadWriteOnce"]
    storage_class_name = "standard"
    resources {
      requests = {
        storage = var.postgres_storage_size
      }
    }
  }
  wait_until_bound = false
}
