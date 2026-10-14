"""Regras de validação usadas pelas tasks da DAG de ingestão do BanVic.

As funções recebem apenas dados simples (caminhos, contratos e uma função que
executa SQL), o que permite testá-las fora do Airflow (ver tests/).
"""

from __future__ import annotations

import csv
import hashlib
import logging
from collections.abc import Callable, Sequence
from pathlib import Path
from typing import Any

from banvic.contrato import RELACIONAMENTOS, SCHEMA_DESTINO, Tabela

log = logging.getLogger(__name__)

# Assinatura: executar(sql, parametros) -> lista de linhas
ExecutorSQL = Callable[[str, Sequence[Any] | None], list[tuple]]


class ContratoViolado(ValueError):
    """A origem ou o destino não respeitam o contrato da tabela."""


def perfilar_arquivo(diretorio: str, tabela: Tabela) -> dict[str, Any]:
    """Valida o cabeçalho do CSV contra o contrato e devolve um perfil pequeno.

    O retorno vai para o XCom, então contém apenas metadados (nunca os dados).
    """
    caminho = Path(diretorio) / tabela.arquivo
    sha256 = hashlib.sha256()
    linhas = 0

    with caminho.open("r", encoding="utf-8", newline="") as arquivo:
        leitor = csv.reader(arquivo, strict=True)
        cabecalho = next(leitor, None)
        if cabecalho is None:
            raise ContratoViolado(f"{caminho} está vazio (sem cabeçalho).")

        esperado = list(tabela.colunas)
        if cabecalho != esperado:
            faltando = sorted(set(esperado) - set(cabecalho))
            inesperadas = sorted(set(cabecalho) - set(esperado))
            raise ContratoViolado(
                f"Cabeçalho de {tabela.arquivo} diverge do contrato. "
                f"Faltando: {faltando or '-'} | Inesperadas: {inesperadas or '-'} | "
                f"Ordem recebida: {cabecalho}"
            )

        for numero, registro in enumerate(leitor, start=2):
            if len(registro) != len(esperado):
                raise ContratoViolado(
                    f"{tabela.arquivo}, linha {numero}: {len(registro)} campos "
                    f"(esperado {len(esperado)})."
                )
            linhas += 1

    with caminho.open("rb") as binario:
        for bloco in iter(lambda: binario.read(1024 * 1024), b""):
            sha256.update(bloco)

    if linhas == 0:
        raise ContratoViolado(f"{tabela.arquivo} não possui registros.")

    perfil = {
        "tabela": tabela.nome,
        "arquivo": str(caminho),
        "linhas": linhas,
        "sha256": sha256.hexdigest(),
        "bytes": caminho.stat().st_size,
    }
    log.info("Perfil da origem: %s", perfil)
    return perfil


def validar_destino(
    executar: ExecutorSQL, tabela: Tabela, linhas_origem: int
) -> dict[str, Any]:
    """Confere volume, unicidade da chave e tipos das colunas no PostgreSQL."""
    tipos = dict(
        executar(
            """
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
            """,
            (SCHEMA_DESTINO, tabela.nome),
        )
    )
    if not tipos:
        raise ContratoViolado(f"Tabela {SCHEMA_DESTINO}.{tabela.nome} não existe.")

    divergencias = {
        coluna: {"esperado": esperado, "encontrado": tipos.get(coluna)}
        for coluna, esperado in tabela.colunas.items()
        if tipos.get(coluna) != esperado
    }
    if divergencias:
        raise ContratoViolado(
            f"Tipos divergentes em {SCHEMA_DESTINO}.{tabela.nome}: {divergencias}"
        )

    # Identificadores vêm do contrato (constantes do código), nunca de input.
    total, chaves_distintas = executar(
        f'SELECT COUNT(*), COUNT(DISTINCT "{tabela.chave}") '
        f'FROM "{SCHEMA_DESTINO}"."{tabela.nome}"',
        None,
    )[0]

    if total != linhas_origem:
        raise ContratoViolado(
            f"{SCHEMA_DESTINO}.{tabela.nome}: origem={linhas_origem} destino={total}"
        )
    if chaves_distintas != total:
        raise ContratoViolado(
            f"{SCHEMA_DESTINO}.{tabela.nome}: chave {tabela.chave} duplicada "
            f"({total - chaves_distintas} repetições)."
        )

    resultado = {"tabela": tabela.nome, "linhas_destino": total, "tipos_ok": True}
    log.info("Destino validado: %s", resultado)
    return resultado


def verificar_relacionamentos(executar: ExecutorSQL) -> dict[str, int]:
    """Conta registros órfãos para cada relacionamento do modelo conceitual.

    A camada raw preserva a origem como ela é, portanto órfãos geram ALERTA
    (e ficam registrados no log/XCom) em vez de bloquear o pipeline.
    """
    orfaos: dict[str, int] = {}
    for filha, fk, pai, pk in RELACIONAMENTOS:
        quantidade = executar(
            f'SELECT COUNT(*) FROM "{SCHEMA_DESTINO}"."{filha}" f '
            f'LEFT JOIN "{SCHEMA_DESTINO}"."{pai}" p ON f."{fk}" = p."{pk}" '
            f'WHERE p."{pk}" IS NULL',
            None,
        )[0][0]
        relacao = f"{filha}.{fk} -> {pai}.{pk}"
        orfaos[relacao] = quantidade
        if quantidade:
            log.warning("ALERTA de integridade: %s órfão(s) em %s", quantidade, relacao)
        else:
            log.info("OK: %s", relacao)
    return orfaos
