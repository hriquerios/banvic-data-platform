output "kubectl_context" {
  description = "Contexto do kubectl para o cluster criado."
  value       = "kind-${kind_cluster.banvic.name}"
}

output "namespaces" {
  description = "Namespaces do projeto."
  value = {
    dados        = kubernetes_namespace_v1.dados.metadata[0].name
    orquestracao = kubernetes_namespace_v1.orquestracao.metadata[0].name
  }
}

output "airflow_url" {
  description = "Endereço da UI do Airflow."
  value       = "http://localhost:${var.airflow_host_port}"
}

output "airflow_admin_user" {
  description = "Usuário administrador do Airflow."
  value       = var.airflow_admin_user
}

output "airflow_admin_password" {
  description = "Senha do admin do Airflow. Exibir com: terraform output -raw airflow_admin_password"
  value       = local.airflow_admin_password
  sensitive   = true
}

output "postgres_externo" {
  description = "Conexão para DBeaver/psql a partir do seu computador (usuário somente leitura)."
  value = {
    host     = "localhost"
    port     = var.postgres_host_port
    database = var.dw_database
    user     = var.reader_db_user
    senha    = "terraform output -raw postgres_reader_password"
  }
}

output "postgres_reader_password" {
  description = "Senha do usuário somente leitura do schema raw."
  value       = local.reader_db_password
  sensitive   = true
}

output "imagens" {
  description = "Imagens construídas e carregadas no Kind."
  value = {
    airflow = local.airflow_image
    meltano = local.meltano_image
  }
}

output "dag_id" {
  description = "DAG de ingestão."
  value       = "banvic_erp_ingestao_raw"
}
