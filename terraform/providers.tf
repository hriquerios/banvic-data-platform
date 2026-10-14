# O cluster Kind é criado neste mesmo state; os providers Kubernetes e Helm
# usam as credenciais exportadas pelo recurso kind_cluster (nenhum kubeconfig
# precisa ser editado manualmente).

provider "kind" {}

provider "kubernetes" {
  host                   = kind_cluster.banvic.endpoint
  client_certificate     = kind_cluster.banvic.client_certificate
  client_key             = kind_cluster.banvic.client_key
  cluster_ca_certificate = kind_cluster.banvic.cluster_ca_certificate
}

provider "helm" {
  kubernetes = {
    host                   = kind_cluster.banvic.endpoint
    client_certificate     = kind_cluster.banvic.client_certificate
    client_key             = kind_cluster.banvic.client_key
    cluster_ca_certificate = kind_cluster.banvic.cluster_ca_certificate
  }
}

provider "random" {}
