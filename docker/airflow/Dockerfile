# -----------------------------------------------------------------------------
# Imagem do Airflow do BanVic
#   Base oficial (já inclui os providers cncf.kubernetes, postgres, standard e
#   fab) + as DAGs do projeto. A carga de dados roda em OUTRA imagem
#   (docker/meltano), em pods isolados - o Airflow apenas orquestra.
#
# Build (feito automaticamente pelo Terraform, a partir da raiz do projeto):
#   docker build -f docker/airflow/Dockerfile -t banvic/airflow:<tag> .
# -----------------------------------------------------------------------------
ARG AIRFLOW_VERSION=3.2.2
ARG PYTHON_VERSION=3.12
FROM apache/airflow:${AIRFLOW_VERSION}-python${PYTHON_VERSION}

# A imagem oficial já roda como o usuário não-root "airflow" (uid 50000).
USER airflow
WORKDIR /opt/airflow

# Permite "from banvic import ..." nas DAGs em todos os processos
# (dag-processor, scheduler/task runner e Jobs auxiliares).
ENV PYTHONPATH=/opt/airflow/dags

# Falha o build cedo se algum provider usado pelas DAGs não estiver na imagem.
RUN python -c "import airflow.providers.cncf.kubernetes, airflow.providers.postgres, airflow.providers.standard, airflow.providers.fab; print('providers OK')"

COPY --chown=airflow:root dags/ /opt/airflow/dags/

# Garante que a DAG é importável dentro da imagem (erro de sintaxe/import
# quebra o build em vez de aparecer só na UI).
RUN python -c "import ast, pathlib; [ast.parse(p.read_text(encoding='utf-8')) for p in pathlib.Path('/opt/airflow/dags').rglob('*.py')]; print('DAGs OK')"
