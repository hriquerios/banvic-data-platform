"""Testes de integridade da DAG (executados apenas onde o Airflow estiver instalado)."""

from __future__ import annotations

from pathlib import Path

import pytest

pytest.importorskip("airflow")

from airflow.dag_processing.dagbag import DagBag  # noqa: E402

from banvic.contrato import TABELAS  # noqa: E402

DAGS = Path(__file__).resolve().parents[1] / "dags"
DAG_ID = "banvic_erp_ingestao_raw"


@pytest.fixture(scope="module")
def dag():
    bag = DagBag(dag_folder=str(DAGS), include_examples=False)
    assert bag.import_errors == {}
    return bag.dags[DAG_ID]


def test_parametros_essenciais(dag):
    assert dag.catchup is False
    assert dag.max_active_runs == 1
    assert dag.start_date is not None
    assert {"banvic", "elt", "meltano"} <= set(dag.tags)
    assert dag.doc_md


def test_uma_taskgroup_por_tabela(dag):
    for tabela in TABELAS:
        ids = {t.task_id for t in dag.tasks if t.task_id.startswith(f"{tabela.nome}.")}
        assert ids == {
            f"{tabela.nome}.aguardar_arquivo",
            f"{tabela.nome}.validar_contrato",
            f"{tabela.nome}.carregar_raw",
            f"{tabela.nome}.validar_carga",
        }


def test_retries_vem_do_default_args(dag):
    carga = dag.get_task("contas.carregar_raw")
    assert carga.retries >= 1 and carga.retry_exponential_backoff


def test_configuracao_operacional_via_jinja(dag):
    carga = dag.get_task("contas.carregar_raw")
    assert "{{ var.value." in carga.image
    assert carga.arguments == ["run", "tap-banvic-contas", "target-postgres"]


def test_sem_credenciais_no_codigo():
    proibidos = ("password=", "senha=", "postgresql://")
    for arquivo in DAGS.rglob("*.py"):
        conteudo = arquivo.read_text(encoding="utf-8").lower()
        assert not any(p in conteudo for p in proibidos), arquivo
