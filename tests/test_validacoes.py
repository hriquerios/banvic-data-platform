"""Testes unitários das regras de validação (não dependem de Airflow nem de banco)."""

from __future__ import annotations

from pathlib import Path

import pytest

from banvic.contrato import DATA, INTEIRO, TABELAS, TEXTO, Tabela
from banvic.validacoes import (
    ContratoViolado,
    perfilar_arquivo,
    validar_destino,
    verificar_relacionamentos,
)

DADOS = Path(__file__).resolve().parents[1] / "data" / "banvic_erp"

AGENCIAS = Tabela(
    nome="agencias",
    chave="cod_agencia",
    colunas={"cod_agencia": INTEIRO, "nome": TEXTO, "data_abertura": DATA},
)


def _csv(tmp_path: Path, conteudo: str) -> Path:
    (tmp_path / "agencias.csv").write_text(conteudo, encoding="utf-8")
    return tmp_path


@pytest.mark.parametrize("tabela", TABELAS, ids=lambda t: t.nome)
def test_arquivos_reais_respeitam_o_contrato(tabela):
    perfil = perfilar_arquivo(str(DADOS), tabela)
    assert perfil["linhas"] > 0
    assert len(perfil["sha256"]) == 64


def test_cabecalho_divergente_falha(tmp_path):
    pasta = _csv(tmp_path, "cod_agencia,nome_agencia,data_abertura\n1,Matriz,2010-01-01\n")
    with pytest.raises(ContratoViolado, match="Faltando: \\['nome'\\]"):
        perfilar_arquivo(str(pasta), AGENCIAS)


def test_linha_com_campos_a_mais_falha(tmp_path):
    pasta = _csv(tmp_path, "cod_agencia,nome,data_abertura\n1,Matriz,2010-01-01,extra\n")
    with pytest.raises(ContratoViolado, match="linha 2"):
        perfilar_arquivo(str(pasta), AGENCIAS)


def test_arquivo_sem_registros_falha(tmp_path):
    pasta = _csv(tmp_path, "cod_agencia,nome,data_abertura\n")
    with pytest.raises(ContratoViolado, match="não possui registros"):
        perfilar_arquivo(str(pasta), AGENCIAS)


class BancoFalso:
    """Simula as consultas feitas pelas validações de destino."""

    def __init__(self, tipos, total, distintos, orfaos=0):
        self.tipos, self.total, self.distintos, self.orfaos = tipos, total, distintos, orfaos

    def __call__(self, sql, parametros=None):
        if "information_schema" in sql:
            return list(self.tipos.items())
        if "LEFT JOIN" in sql:
            return [(self.orfaos,)]
        return [(self.total, self.distintos)]


def test_destino_ok():
    banco = BancoFalso(dict(AGENCIAS.colunas), total=10, distintos=10)
    assert validar_destino(banco, AGENCIAS, 10)["linhas_destino"] == 10


def test_destino_com_tipo_errado_falha():
    tipos = dict(AGENCIAS.colunas) | {"data_abertura": "text"}
    with pytest.raises(ContratoViolado, match="Tipos divergentes"):
        validar_destino(BancoFalso(tipos, 10, 10), AGENCIAS, 10)


def test_destino_com_volume_diferente_falha():
    with pytest.raises(ContratoViolado, match="origem=10 destino=9"):
        validar_destino(BancoFalso(dict(AGENCIAS.colunas), 9, 9), AGENCIAS, 10)


def test_destino_com_chave_duplicada_falha():
    with pytest.raises(ContratoViolado, match="duplicada"):
        validar_destino(BancoFalso(dict(AGENCIAS.colunas), 10, 8), AGENCIAS, 10)


def test_tabela_inexistente_falha():
    with pytest.raises(ContratoViolado, match="não existe"):
        validar_destino(BancoFalso({}, 0, 0), AGENCIAS, 10)


def test_relacionamentos_nao_bloqueiam():
    resultado = verificar_relacionamentos(BancoFalso({}, 0, 0, orfaos=3))
    assert all(qtd == 3 for qtd in resultado.values())
