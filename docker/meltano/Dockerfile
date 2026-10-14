# -----------------------------------------------------------------------------
# Imagem de ingestão do BanVic: Meltano + tap-csv + target-postgres
#   Executada pelo Airflow via KubernetesPodOperator, um pod por tabela.
#   Não contém dados (os CSVs chegam por volume) nem credenciais (chegam por
#   Secret Kubernetes como variáveis de ambiente).
#
# Build (feito automaticamente pelo Terraform, a partir da raiz do projeto):
#   docker build -f docker/meltano/Dockerfile -t banvic/meltano:<tag> .
# -----------------------------------------------------------------------------
FROM python:3.12-slim-bookworm

ARG MELTANO_VERSION=4.4.1

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    TZ=UTC \
    MELTANO_SEND_ANONYMOUS_USAGE_STATS=false \
    MELTANO_PROJECT_ROOT=/opt/meltano

# git: o tap-csv é instalado a partir de uma tag do repositório oficial.
RUN apt-get update \
    && apt-get install -y --no-install-recommends git \
    && rm -rf /var/lib/apt/lists/*

# Usuário sem privilégios (mesmo uid/gid usado no securityContext do pod).
RUN groupadd --gid 10001 meltano \
    && useradd --uid 10001 --gid 10001 --create-home --shell /usr/sbin/nologin meltano

RUN pip install "meltano==${MELTANO_VERSION}"

WORKDIR /opt/meltano

# Somente a definição do projeto (o .env local nunca entra na imagem).
# Os plugins são declarados no próprio meltano.yml: não há consulta ao Meltano Hub.
COPY meltano/meltano.yml ./meltano.yml

# Instala tap-csv e target-postgres no build: o pod não depende de internet em runtime.
RUN meltano install \
    && chown -R meltano:meltano /opt/meltano

USER 10001

ENTRYPOINT ["meltano"]
CMD ["--help"]
