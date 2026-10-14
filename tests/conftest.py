"""Torna o pacote dags/banvic importável nos testes (sem precisar do Airflow)."""

import sys
from pathlib import Path

RAIZ = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(RAIZ / "dags"))
