# =============================================================================
# Imagens Docker customizadas
#
# A tag de cada imagem é um hash do conteúdo que ela empacota. Assim:
#   * se nada mudou, o Terraform não reconstrói nada (idempotente);
#   * se uma DAG ou o meltano.yml mudar, a tag muda, a imagem é reconstruída,
#     carregada no Kind e o Helm faz o rollout automaticamente.
# =============================================================================

locals {
  airflow_image_files = sort(concat(
    tolist(fileset(local.project_root, "dags/**/*.py")),
    tolist(fileset(local.project_root, "dags/.airflowignore")),
    tolist(fileset(local.project_root, "docker/airflow/*")),
  ))

  meltano_image_files = sort(concat(
    tolist(fileset(local.project_root, "meltano/meltano.yml")),
    tolist(fileset(local.project_root, "docker/meltano/*")),
  ))

  airflow_image_tag = substr(sha1(join(",", [for f in local.airflow_image_files : "${f}=${filesha1("${local.project_root}/${f}")}"])), 0, 12)
  meltano_image_tag = substr(sha1(join(",", [for f in local.meltano_image_files : "${f}=${filesha1("${local.project_root}/${f}")}"])), 0, 12)

  airflow_image = "${var.airflow_image_repository}:${local.airflow_image_tag}"
  meltano_image = "${var.meltano_image_repository}:${local.meltano_image_tag}"
}

# Imagem do Airflow: imagem oficial + DAGs e módulos auxiliares.
resource "terraform_data" "airflow_image" {
  triggers_replace = [local.airflow_image, kind_cluster.banvic.id]

  provisioner "local-exec" {
    working_dir = local.project_root
    command     = "docker build -f docker/airflow/Dockerfile -t ${local.airflow_image} ."
  }

  provisioner "local-exec" {
    command = "kind load docker-image ${local.airflow_image} --name ${kind_cluster.banvic.name}"
  }
}

# Imagem de ingestão: Meltano com tap-csv e target-postgres já instalados.
resource "terraform_data" "meltano_image" {
  triggers_replace = [local.meltano_image, kind_cluster.banvic.id]

  provisioner "local-exec" {
    working_dir = local.project_root
    command     = "docker build -f docker/meltano/Dockerfile -t ${local.meltano_image} ."
  }

  provisioner "local-exec" {
    command = "kind load docker-image ${local.meltano_image} --name ${kind_cluster.banvic.name}"
  }
}
