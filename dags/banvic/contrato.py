"""Contrato de dados das 7 tabelas do ERP do BanVic.

Fonte única de verdade usada pela DAG para:
  * gerar uma TaskGroup por tabela;
  * validar o cabeçalho de cada CSV antes da carga (colunas e ordem);
  * validar, depois da carga, se as colunas chegaram no PostgreSQL com o tipo
    esperado (mesma tipagem declarada nos stream_maps do meltano/meltano.yml).
"""

from __future__ import annotations

from dataclasses import dataclass, field

# Tipos como aparecem em information_schema.columns.data_type
INTEIRO = "bigint"
TEXTO = "text"
DECIMAL = "numeric"
DATA = "date"
DATA_HORA = "timestamp without time zone"  # valores em UTC (sufixo " UTC" na origem)

# Nomes de objetos (não sensíveis) compartilhados com a infraestrutura.
SCHEMA_DESTINO = "raw"
DW_CONN_ID = "banvic_dw"
FS_CONN_ID = "fs_default"
LOADER_SECRET = "banvic-loader-credentials"
FONTE_PVC = "banvic-fonte-erp"
FONTE_MOUNT_PATH = "/data/banvic_erp"
INGESTAO_SERVICE_ACCOUNT = "banvic-ingestao"


@dataclass(frozen=True)
class Tabela:
    """Contrato de uma tabela do ERP."""

    nome: str
    chave: str
    colunas: dict[str, str] = field(default_factory=dict)

    @property
    def arquivo(self) -> str:
        return f"{self.nome}.csv"

    @property
    def extractor(self) -> str:
        """Extractor Meltano dedicado (herda de tap-csv, ver meltano.yml)."""
        return "tap-banvic-" + self.nome.replace("_", "-")

    @property
    def slug(self) -> str:
        """Nome compatível com DNS (usado no nome do pod de ingestão)."""
        return self.nome.replace("_", "-")


TABELAS: tuple[Tabela, ...] = (
    Tabela(
        nome="agencias",
        chave="cod_agencia",
        colunas={
            "cod_agencia": INTEIRO,
            "nome": TEXTO,
            "endereco": TEXTO,
            "cidade": TEXTO,
            "uf": TEXTO,
            "data_abertura": DATA,
            "tipo_agencia": TEXTO,
        },
    ),
    Tabela(
        nome="clientes",
        chave="cod_cliente",
        colunas={
            "cod_cliente": INTEIRO,
            "primeiro_nome": TEXTO,
            "ultimo_nome": TEXTO,
            "email": TEXTO,
            "tipo_cliente": TEXTO,
            "data_inclusao": DATA_HORA,
            "cpfcnpj": TEXTO,
            "data_nascimento": DATA,
            "endereco": TEXTO,
            "cep": TEXTO,
        },
    ),
    Tabela(
        nome="colaboradores",
        chave="cod_colaborador",
        colunas={
            "cod_colaborador": INTEIRO,
            "primeiro_nome": TEXTO,
            "ultimo_nome": TEXTO,
            "email": TEXTO,
            "cpf": TEXTO,
            "data_nascimento": DATA,
            "endereco": TEXTO,
            "cep": TEXTO,
        },
    ),
    Tabela(
        nome="colaborador_agencia",
        chave="cod_colaborador",
        colunas={
            "cod_colaborador": INTEIRO,
            "cod_agencia": INTEIRO,
        },
    ),
    Tabela(
        nome="contas",
        chave="num_conta",
        colunas={
            "num_conta": INTEIRO,
            "cod_cliente": INTEIRO,
            "cod_agencia": INTEIRO,
            "cod_colaborador": INTEIRO,
            "tipo_conta": TEXTO,
            "data_abertura": DATA_HORA,
            "saldo_total": DECIMAL,
            "saldo_disponivel": DECIMAL,
            "data_ultimo_lancamento": DATA_HORA,
        },
    ),
    Tabela(
        nome="propostas_credito",
        chave="cod_proposta",
        colunas={
            "cod_proposta": INTEIRO,
            "cod_cliente": INTEIRO,
            "cod_colaborador": INTEIRO,
            "data_entrada_proposta": DATA_HORA,
            "taxa_juros_mensal": DECIMAL,
            "valor_proposta": DECIMAL,
            "valor_financiamento": DECIMAL,
            "valor_entrada": DECIMAL,
            "valor_prestacao": DECIMAL,
            "quantidade_parcelas": INTEIRO,
            "carencia": INTEIRO,
            "status_proposta": TEXTO,
        },
    ),
    Tabela(
        nome="transacoes",
        chave="cod_transacao",
        colunas={
            "cod_transacao": INTEIRO,
            "num_conta": INTEIRO,
            "data_transacao": DATA_HORA,
            "nome_transacao": TEXTO,
            "valor_transacao": DECIMAL,
        },
    ),
)

# Relacionamentos do modelo conceitual verificados após a carga
# (tabela_filha, coluna_fk, tabela_pai, coluna_pk).
RELACIONAMENTOS: tuple[tuple[str, str, str, str], ...] = (
    ("contas", "cod_cliente", "clientes", "cod_cliente"),
    ("contas", "cod_agencia", "agencias", "cod_agencia"),
    ("contas", "cod_colaborador", "colaboradores", "cod_colaborador"),
    ("transacoes", "num_conta", "contas", "num_conta"),
    ("propostas_credito", "cod_cliente", "clientes", "cod_cliente"),
    ("propostas_credito", "cod_colaborador", "colaboradores", "cod_colaborador"),
    ("colaborador_agencia", "cod_colaborador", "colaboradores", "cod_colaborador"),
    ("colaborador_agencia", "cod_agencia", "agencias", "cod_agencia"),
)
