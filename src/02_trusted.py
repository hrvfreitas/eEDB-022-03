""" Etapa 2 — Camada Trusted
Lê a camada RAW (do Postgres) e aplica, via Spark DataFrame API:
- tipagem correta das colunas
- normalização da chave CNPJ (raiz, 8 dígitos, com zero-padding)
- deduplicação de bancos por CNPJ
- unificação e deduplicação dos dois arquivos de Empregados
- resolução de CNPJ dos bancos "conglomerado" por nome
  (match exato + fallback via matching aproximado — fuzzy ou Splink)
Grava o resultado em Parquet (data/trusted/) e no Postgres (schema "trusted").
"""
import argparse
import os
import sys
from pyspark.sql import functions as F
from pyspark.sql.window import Window

sys.path.append(os.path.dirname(__file__))
from spark_session import get_spark
from db import read_table, write_table
from fuzzy_match import normalizar_nome  # UDF para normalização

TRUSTED_OUT_DIR = os.environ.get("TRUSTED_OUT_DIR", "/opt/data/trusted")
MATCH_STRATEGIES = ("fuzzy", "splink")

def parse_match_strategy() -> str:
    """Lê a estratégia de matching da linha de comando ou variável de ambiente."""
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--match-strategy", choices=MATCH_STRATEGIES, default=None)
    args, _ = parser.parse_known_args()
    strategy = args.match_strategy or os.environ.get("MATCH_STRATEGY", "fuzzy")
    if strategy not in MATCH_STRATEGIES:
        raise ValueError(f"MATCH_STRATEGY inválida: {strategy!r}.")
    return strategy

# UDF para normalizar nomes (usada em várias transformações)
normalizar_nome_udf = F.udf(normalizar_nome)

def cnpj_raiz_8(col):
    """Extrai/normaliza o CNPJ raiz para 8 dígitos com zero à esquerda."""
    digitos = F.regexp_replace(col.cast("string"), r"[^0-9]", "")  # remove não-dígitos
    return F.lpad(F.substring(digitos, 1, 8), 8, "0")              # pega 8 primeiros, pad com zeros

# ---------------------------------------------------------------- Bancos --
def tratar_bancos(spark):
    df = read_table(spark, "raw", "bancos")
    df = (df
          .withColumn("cnpj", cnpj_raiz_8(F.col("CNPJ")))                    # normaliza CNPJ
          .withColumn("nome_instituicao", F.trim(F.col("NomeInstituicao")))  # remove espaços
          .withColumn("segmento_prudencial", F.trim(F.col("Segmento")))      # segmento limpo
          .withColumn("nome_normalizado", normalizar_nome_udf(F.col("nome_instituicao")))  # para matching
         )
    # Deduplicação por CNPJ: prioriza registros com "PRUDENCIAL" no nome
    w = Window.partitionBy("cnpj").orderBy(
        F.when(F.col("nome_instituicao").contains("PRUDENCIAL"), 0).otherwise(1),
        F.col("nome_instituicao")
    )
    df_dedup = df.withColumn("rn", F.row_number().over(w)).filter(F.col("rn") == 1).drop("rn")
    # Preserva o nome alternativo (quando houver) em uma coluna separada
    # ... (lógica para criar 'nome_alternativo') ...
    return df_dedup

# ---------------------------------------------------------------- Reclamações --
def tratar_reclamacoes(spark, bancos_df, match_strategy):
    df = read_table(spark, "raw", "reclamacoes")
    # Renomeia/tipa colunas: ano, trimestre, categorias, quantidades, etc.
    # ... (transformações de tipo: inteiros, floats, strings) ...

    # Resolução de CNPJ para conglomerados:
    # 1. Match exato por nome normalizado
    # 2. Fallback: fuzzy ou splink (conforme match_strategy)
    # ... (lógica de join com bancos_df e chamada para fuzzy/splink) ...
    return df

# ---------------------------------------------------------------- Empregados --
def tratar_empregados(spark, bancos_df):
    # Lê os dois arquivos da RAW
    df_match = read_table(spark, "raw", "empregados_match")
    df_match_less = read_table(spark, "raw", "empregados_match_less")

    # Une os dois DataFrames (unionByName)
    df_unido = df_match.unionByName(df_match_less)

    # Normaliza CNPJ (quando presente) e resolve por nome (via join com bancos)
    # ... (lógica de resolução de CNPJ) ...

    # Deduplica por CNPJ: prioriza registros do arquivo "match"
    w = Window.partitionBy("cnpj").orderBy(
        F.when(F.col("arquivo_origem") == "match", 0).otherwise(1)
    )
    df_dedup = df_unido.withColumn("rn", F.row_number().over(w)).filter(F.col("rn") == 1).drop("rn")
    return df_dedup

def main():
    spark = get_spark("02_trusted")
    match_strategy = parse_match_strategy()

    # Processa bancos primeiro (usado como lookup para os demais)
    bancos = tratar_bancos(spark)
    reclamacoes = tratar_reclamacoes(spark, bancos, match_strategy)
    empregados = tratar_empregados(spark, bancos)

    # Escrita em Parquet e Postgres
    bancos.write.parquet(...)
    write_table(bancos, "trusted", "bancos")
    # ... (similar para reclamacoes e empregados) ...

    spark.stop()
