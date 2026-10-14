"""Callbacks de monitoramento da DAG de ingestão."""

from __future__ import annotations

import logging
from typing import Any

log = logging.getLogger("banvic.monitoramento")


def alertar_falha(context: dict[str, Any]) -> None:
    """Registra uma mensagem estruturada quando uma task falha (após os retries).

    Ponto único para plugar um canal de alerta (Slack, e-mail, Teams) no futuro
    sem alterar a DAG.
    """
    ti = context.get("task_instance") or context.get("ti")
    log.error(
        "FALHA no pipeline BanVic | dag=%s | task=%s | run_id=%s | tentativa=%s | erro=%s",
        getattr(ti, "dag_id", "?"),
        getattr(ti, "task_id", "?"),
        context.get("run_id"),
        getattr(ti, "try_number", "?"),
        context.get("exception"),
    )


def registrar_retry(context: dict[str, Any]) -> None:
    """Deixa explícito no log quando uma task entra em nova tentativa."""
    ti = context.get("task_instance") or context.get("ti")
    log.warning(
        "RETRY agendado | dag=%s | task=%s | tentativa=%s",
        getattr(ti, "dag_id", "?"),
        getattr(ti, "task_id", "?"),
        getattr(ti, "try_number", "?"),
    )
