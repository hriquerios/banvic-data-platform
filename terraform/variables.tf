# =============================================================================
# Cluster Kubernetes local (Kind)
# =============================================================================
variable "cluster_name" {
  description = "Nome do cluster Kind (o contexto do kubectl será kind-<nome>)."
  type        = string
  default     = "banvic"
}

variable "kind_node_image" {
  description = "Imagem do nó Kind (fixa a versão do Kubernetes para reprodutibilidade)."
  type        = string
  # Imagem padrão do Kind v0.31 (a mesma versão usada pelo provider), fixada por digest.
  default = "kindest/node:v1.35.0@sha256:452d707d4862f52530247495d180205e029056831160e22870e37e3f6c1ac31f"
}

variable "source_data_dir" {
  description = "Diretório no host com os 7 CSVs do ERP. Vazio = <raiz do projeto>/data/banvic_erp."
  type        = string
  default     = ""
}

variable "node_source_path" {
  description = "Caminho, dentro do nó Kind, onde o diretório de dados do host é montado (somente leitura)."
  type        = string
  default     = "/mnt/banvic/erp"
}

variable "airflow_host_port" {
  description = "Porta do seu computador para acessar a UI do Airflow (http://localhost:<porta>)."
  type        = number
  default     = 8080
}

variable "postgres_host_port" {
  description = "Porta do seu computador para acessar o PostgreSQL (DBeaver, psql...)."
  type        = number
  default     = 15432
}

# =============================================================================
# Namespaces
# =============================================================================
variable "namespace_dados" {
  description = "Namespace do armazenamento (PostgreSQL)."
  type        = string
  default     = "banvic-dados"
}

variable "namespace_orquestracao" {
  description = "Namespace da orquestração (Airflow) e dos pods de ingestão (Meltano)."
  type        = string
  default     = "banvic-orquestracao"
}

# =============================================================================
# PostgreSQL
# =============================================================================
variable "postgres_image" {
  description = "Imagem do PostgreSQL."
  type        = string
  default     = "postgres:17-alpine"
}

variable "postgres_storage_size" {
  description = "Tamanho do volume persistente do PostgreSQL."
  type        = string
  default     = "2Gi"
}

variable "postgres_admin_user" {
  description = "Superusuário do PostgreSQL (usado apenas pelo próprio banco e pelo Job de bootstrap)."
  type        = string
  default     = "banvic_admin"
}

variable "dw_database" {
  description = "Banco que recebe os dados do ERP (schema raw)."
  type        = string
  default     = "banvic"
}

variable "airflow_database" {
  description = "Banco de metadados do Airflow."
  type        = string
  default     = "airflow"
}

variable "airflow_db_user" {
  description = "Usuário dono do banco de metadados do Airflow."
  type        = string
  default     = "airflow_meta"
}

variable "loader_db_user" {
  description = "Usuário de escrita no schema raw (usado só pelos pods do Meltano)."
  type        = string
  default     = "banvic_loader"
}

variable "reader_db_user" {
  description = "Usuário somente leitura do schema raw (Connection banvic_dw do Airflow)."
  type        = string
  default     = "banvic_reader"
}

# -----------------------------------------------------------------------------
# Senhas: opcionais. Se não forem informadas (TF_VAR_... ou terraform.tfvars),
# o Terraform gera senhas aleatórias. Todas são sensitive = true, portanto
# nunca aparecem no plan/apply nem nos logs do terminal.
# -----------------------------------------------------------------------------
variable "postgres_admin_password" {
  description = "Senha do superusuário do PostgreSQL. Vazio = gerada automaticamente."
  type        = string
  default     = null
  sensitive   = true
}

variable "airflow_db_password" {
  description = "Senha do usuário de metadados do Airflow. Vazio = gerada automaticamente."
  type        = string
  default     = null
  sensitive   = true
}

variable "loader_db_password" {
  description = "Senha do usuário de carga (Meltano). Vazio = gerada automaticamente."
  type        = string
  default     = null
  sensitive   = true
}

variable "reader_db_password" {
  description = "Senha do usuário somente leitura. Vazio = gerada automaticamente."
  type        = string
  default     = null
  sensitive   = true
}

variable "airflow_admin_password" {
  description = "Senha do usuário admin da UI do Airflow. Vazio = gerada automaticamente."
  type        = string
  default     = null
  sensitive   = true
}

# =============================================================================
# Airflow
# =============================================================================
variable "airflow_version" {
  description = "Versão do Apache Airflow: a appVersion oficial do chart 1.22.0 (deve bater com docker/airflow/Dockerfile)."
  type        = string
  default     = "3.2.2"
}

variable "airflow_chart_version" {
  description = "Versão do Helm chart oficial do Apache Airflow."
  type        = string
  default     = "1.22.0"
}

variable "airflow_image_repository" {
  description = "Nome da imagem customizada do Airflow (DAGs embarcadas)."
  type        = string
  default     = "banvic/airflow"
}

variable "meltano_image_repository" {
  description = "Nome da imagem de ingestão (Meltano + plugins instalados)."
  type        = string
  default     = "banvic/meltano"
}

variable "airflow_admin_user" {
  description = "Usuário administrador da UI do Airflow."
  type        = string
  default     = "admin"
}

variable "airflow_admin_email" {
  description = "E-mail do usuário administrador do Airflow."
  type        = string
  default     = "admin@banvic.local"
}

variable "airflow_logs_storage_size" {
  description = "Tamanho do volume persistente de logs do Airflow."
  type        = string
  default     = "2Gi"
}

variable "airflow_log_retention_days" {
  description = "Dias de retenção dos logs de tasks (log-groomer do chart)."
  type        = number
  default     = 7
}

variable "airflow_parallelism" {
  description = "Máximo de tasks simultâneas no Airflow (LocalExecutor)."
  type        = number
  default     = 6
}
