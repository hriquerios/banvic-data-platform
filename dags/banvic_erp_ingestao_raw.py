"""
### BanVic | Ingestão do ERP para a camada RAW

Pipeline **ELT** que replica as 7 tabelas do ERP do Banco Vitória (arquivos CSV
legados) para o schema `raw` do PostgreSQL, base centralizada para o dashboard
comercial e para as análises da Diretoria.

**Fluxo por tabela** (uma TaskGroup independente para cada uma das 7 tabelas):

1. `aguardar_arquivo` – *FileSensor* aguarda o CSV no volume de origem;
2. `validar_contrato` – confere cabeçalho/estrutura e devolve um perfil pequeno
   (linhas, hash) via XCom;
3. `carregar_raw` – *KubernetesPodOperator* sobe um pod isolado com o Meltano
   (`tap-banvic-<tabela>` ➜ `target-postgres`, carga *Full Refresh* com
   `load_method: overwrite`);
4. `validar_carga` – compara volume, unicidade da chave e **tipos** no destino.

Ao final, `verificar_integridade_referencial` mede registros órfãos entre as
tabelas (alerta, não bloqueia — a camada raw preserva a origem).

**Resiliência**: retries com backoff exponencial em todas as tasks
(`default_args`), timeout por task, `max_active_runs=1` e cargas idempotentes
(reexecutar produz o mesmo estado final).

**Segurança**: a DAG não contém credenciais. A senha de escrita existe apenas
no Secret montado nos pods de ingestão; o Airflow lê o banco com um usuário
somente-leitura via Connection `banvic_dw` (variável `AIRFLOW_CONN_BANVIC_DW`).
"""

from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.sensors.filesystem import FileSensor
from airflow.sdk import TaskGroup, chain, dag, task
from kubernetes.client import models as k8s

from banvic.callbacks import alertar_falha, registrar_retry
from banvic.contrato import (
    DW_CONN_ID,
    FONTE_MOUNT_PATH,
    FONTE_PVC,
    FS_CONN_ID,
    INGESTAO_SERVICE_ACCOUNT,
    LOADER_SECRET,
    TABELAS,
    Tabela,
)

# Valores operacionais lidos SOMENTE em tempo de execução (Jinja), nunca no
# parse da DAG. As Variables são definidas por variáveis de ambiente
# AIRFLOW_VAR_* no Helm (ver terraform/templates/airflow-values.yaml.tftpl).
DIRETORIO_ORIGEM = "{{ var.value.banvic_source_dir }}"
IMAGEM_INGESTAO = "{{ var.value.banvic_ingestion_image }}"
NAMESPACE_INGESTAO = "{{ var.value.banvic_k8s_namespace }}"

default_args = {
    "owner": "henrique.rios",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=1),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=10),
    "execution_timeout": timedelta(minutes=20),
    "on_failure_callback": alertar_falha,
    "on_retry_callback": registrar_retry,
}

# Recursos do pod de ingestão (Meltano) - dimensionados para o cluster local.
RECURSOS_INGESTAO = k8s.V1ResourceRequirements(
    requests={"cpu": "200m", "memory": "320Mi"},
    limits={"cpu": "1", "memory": "640Mi"},
)


def _contrato(nome_tabela: str) -> Tabela:
    return next(t for t in TABELAS if t.nome == nome_tabela)


def _executor_sql(conn_id: str):
    """Cria a função executar(sql, parametros) usada pelas validações."""
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    hook = PostgresHook(postgres_conn_id=conn_id)
    return lambda sql, parametros=None: hook.get_records(sql, parameters=parametros)


def _pod_de_ingestao(tabela: Tabela) -> KubernetesPodOperator:
    """Task de carga: um pod efêmero e isolado executando o Meltano."""
    volume_origem = k8s.V1Volume(
        name="fonte-erp",
        persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(
            claim_name=FONTE_PVC, read_only=True
        ),
    )
    return KubernetesPodOperator(
        task_id="carregar_raw",
        name=f"meltano-{tabela.slug}",
        namespace=NAMESPACE_INGESTAO,
        image=IMAGEM_INGESTAO,
        image_pull_policy="IfNotPresent",
        cmds=["meltano"],
        arguments=["run", tabela.extractor, "target-postgres"],
        env_vars={
            "MELTANO_ENVIRONMENT": "kubernetes",
            "BANVIC_SOURCE_DIR": FONTE_MOUNT_PATH,
        },
        # Credenciais de escrita: somente este pod recebe o Secret.
        env_from=[
            k8s.V1EnvFromSource(
                secret_ref=k8s.V1SecretEnvSource(name=LOADER_SECRET)
            )
        ],
        volumes=[volume_origem],
        volume_mounts=[
            k8s.V1VolumeMount(
                name="fonte-erp", mount_path=FONTE_MOUNT_PATH, read_only=True
            )
        ],
        service_account_name=INGESTAO_SERVICE_ACCOUNT,
        automount_service_account_token=False,
        security_context=k8s.V1PodSecurityContext(
            run_as_non_root=True, run_as_user=10001, run_as_group=10001
        ),
        container_security_context=k8s.V1SecurityContext(
            allow_privilege_escalation=False,
            capabilities=k8s.V1Capabilities(drop=["ALL"]),
        ),
        container_resources=RECURSOS_INGESTAO,
        labels={"app.kubernetes.io/part-of": "banvic", "banvic/tabela": tabela.slug},
        in_cluster=True,
        get_logs=True,
        startup_timeout_seconds=300,
        on_finish_action="delete_succeeded_pod",
        reattach_on_restart=True,
        do_xcom_push=False,
    )


@dag(
    dag_id="banvic_erp_ingestao_raw",
    description="ELT diário: 7 tabelas do ERP (CSV) -> PostgreSQL raw via Meltano",
    doc_md=__doc__,
    schedule="0 6 * * *",  # todo dia às 06:00 (antes do expediente comercial)
    start_date=pendulum.datetime(2026, 1, 1, tz="America/Sao_Paulo"),
    catchup=False,
    is_paused_upon_creation=False,
    max_active_runs=1,
    max_active_tasks=4,
    dagrun_timeout=timedelta(hours=1),
    default_args=default_args,
    tags=["banvic", "elt", "raw", "meltano", "postgres", "kubernetes"],
)
def banvic_erp_ingestao_raw():
    inicio = EmptyOperator(task_id="inicio")
    fim = EmptyOperator(task_id="fim")

    @task
    def validar_contrato(nome_tabela: str, diretorio: str) -> dict:
        """Valida o cabeçalho do CSV e devolve um perfil (metadados) da origem."""
        from banvic.validacoes import perfilar_arquivo

        return perfilar_arquivo(diretorio, _contrato(nome_tabela))

    @task
    def validar_carga(perfil_origem: dict) -> dict:
        """Confere volume, chave e tipos da tabela carregada no PostgreSQL."""
        from banvic.validacoes import validar_destino

        return validar_destino(
            _executor_sql(DW_CONN_ID),
            _contrato(perfil_origem["tabela"]),
            perfil_origem["linhas"],
        )

    @task
    def verificar_integridade_referencial() -> dict:
        """Mede registros órfãos entre as tabelas (não bloqueante)."""
        from banvic.validacoes import verificar_relacionamentos

        return verificar_relacionamentos(_executor_sql(DW_CONN_ID))

    grupos = []
    for tabela in TABELAS:
        with TaskGroup(group_id=tabela.nome, tooltip=f"Ingestão de {tabela.arquivo}") as grupo:
            aguardar = FileSensor(
                task_id="aguardar_arquivo",
                fs_conn_id=FS_CONN_ID,
                filepath=f"{DIRETORIO_ORIGEM}/{tabela.arquivo}",
                mode="reschedule",
                poke_interval=30,
                timeout=int(timedelta(minutes=15).total_seconds()),
                retries=0,  # o próprio timeout do sensor já cobre a espera
            )
            perfil = validar_contrato.override(task_id="validar_contrato")(
                nome_tabela=tabela.nome, diretorio=DIRETORIO_ORIGEM
            )
            carga = _pod_de_ingestao(tabela)
            validacao = validar_carga.override(task_id="validar_carga")(perfil)

            chain(aguardar, perfil, carga, validacao)
        grupos.append(grupo)

    integridade = verificar_integridade_referencial()

    chain(inicio, grupos, integridade, fim)


banvic_erp_ingestao_raw()
