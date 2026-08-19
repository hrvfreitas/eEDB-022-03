"""Aguarda o Postgres ficar disponível e executa as 3 etapas do pipeline Spark
em sequência: RAW -> Trusted -> Delivery.
A etapa Trusted aceita a flag --match-strategy (ou MATCH_STRATEGY) para escolher
entre "fuzzy" (RapidFuzz) e "splink" (matching probabilístico).
"""
import argparse
import os
import subprocess
import sys
import time
import psycopg2
from db import DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME

HERE = os.path.dirname(__file__)

def wait_for_postgres(timeout=60):
    """Aguarda o PostgreSQL ficar disponível (até 'timeout' segundos)."""
    start = time.time()
    while time.time() - start < timeout:
        try:
            conn = psycopg2.connect(
                host=DB_HOST, port=DB_PORT, user=DB_USER,
                password=DB_PASSWORD, dbname=DB_NAME, connect_timeout=3,
            )
            conn.close()
            print("Postgres disponível.")
            return
        except Exception:
            print("Aguardando Postgres...")
            time.sleep(2)
    raise RuntimeError("Timeout aguardando o Postgres.")

def run_step(script, extra_args=None):
    """Executa um script Python como subprocesso."""
    print(f"\n=== Executando {script} {' '.join(extra_args or [])} ===")
    subprocess.run(
        [sys.executable, os.path.join(HERE, script), *(extra_args or [])],
        check=True  # levanta exceção se o script falhar
    )

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--match-strategy",
        choices=("fuzzy", "splink"),
        default=None,
        help="Estratégia de matching aproximado de nomes na camada Trusted."
    )
    args = parser.parse_args()

    # Monta argumentos extras para a etapa 02_trusted.py
    trusted_args = ["--match-strategy", args.match_strategy] if args.match_strategy else []

    wait_for_postgres()

    # Executa as três etapas em sequência
    run_step("01_ingest_raw.py")
    run_step("02_trusted.py", trusted_args)
    run_step("03_delivery.py")

    print("\nPipeline completo (RAW -> Trusted -> Delivery) executado com sucesso.")
