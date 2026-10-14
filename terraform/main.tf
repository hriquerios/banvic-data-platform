locals {
  project_root = abspath("${path.root}/..")

  # Diretório dos CSVs no host. Calculado a partir da raiz do projeto, então o
  # mesmo código funciona em qualquer máquina (nenhum caminho pessoal fixo).
  source_dir_host = var.source_data_dir != "" ? abspath(var.source_data_dir) : "${local.project_root}/data/banvic_erp"

  postgres_host = "postgres.${var.namespace_dados}.svc.cluster.local"
  postgres_port = 5432

  # NodePorts fixos (mapeados para as portas do host pelo Kind).
  airflow_node_port  = 30080
  postgres_node_port = 30432

  # Caminho onde os CSVs aparecem dentro dos containers.
  source_mount_path = "/data/banvic_erp"

  labels = {
    "app.kubernetes.io/part-of"    = "banvic-data-platform"
    "app.kubernetes.io/managed-by" = "terraform"
  }
}

# =============================================================================
# Cluster Kubernetes local
# =============================================================================
resource "kind_cluster" "banvic" {
  name           = var.cluster_name
  node_image     = var.kind_node_image
  wait_for_ready = true

  kind_config {
    kind        = "Cluster"
    api_version = "kind.x-k8s.io/v1alpha4"

    node {
      role = "control-plane"

      # Pasta de dados do ERP montada no nó (somente leitura). Daí ela vira um
      # PersistentVolume consumido pelo sensor do Airflow e pelos pods Meltano.
      extra_mounts {
        host_path      = local.source_dir_host
        container_path = var.node_source_path
        read_only      = true
      }

      # UI do Airflow -> http://localhost:<airflow_host_port>
      extra_port_mappings {
        container_port = local.airflow_node_port
        host_port      = var.airflow_host_port
        listen_address = "127.0.0.1"
      }

      # PostgreSQL -> localhost:<postgres_host_port>
      extra_port_mappings {
        container_port = local.postgres_node_port
        host_port      = var.postgres_host_port
        listen_address = "127.0.0.1"
      }
    }
  }
}

# =============================================================================
# Namespaces lógicos (nada roda em default/kube-system)
# =============================================================================
resource "kubernetes_namespace_v1" "dados" {
  metadata {
    name   = var.namespace_dados
    labels = merge(local.labels, { "banvic/camada" = "armazenamento" })
  }
}

resource "kubernetes_namespace_v1" "orquestracao" {
  metadata {
    name   = var.namespace_orquestracao
    labels = merge(local.labels, { "banvic/camada" = "orquestracao-ingestao" })
  }
}
