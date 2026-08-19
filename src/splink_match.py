""" Resolução de CNPJ por nome usando Splink (record linkage probabilístico,
modelo Fellegi-Sunter) rodando sobre o backend Spark do Splink.
É a alternativa estatística ao fuzzy_match.py.
"""
import re
import unicodedata
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, IntegerType

SPLINK_MATCH_PROBABILITY_THRESHOLD = 0.85  # probabilidade mínima para aceitar match

def normalizar_nome(nome: str) -> str:
    """Normalização similar à do fuzzy_match (para consistência)."""
    if nome is None:
        return ""
    nome = str(nome).upper().strip()
    nome = unicodedata.normalize("NFKD", nome).encode("ASCII", "ignore").decode("ASCII")
    nome = re.sub(r"[^A-Z0-9 ]", " ", nome)
    nome = re.sub(r"\s+", " ", nome).strip()
    return nome

def splink_match_dataframe(df_pendente, coluna_nome: str, df_candidatos,
                           coluna_candidato_nome: str, coluna_candidato_id: str, spark):
    """
    df_pendente: Spark DataFrame com nomes (coluna `coluna_nome`) ainda sem CNPJ.
    df_candidatos: Spark DataFrame com a base de referência (bancos).
    Retorna df_pendente com colunas extras: cnpj_splink, splink_match_probability.
    """
    from splink import Linker, SettingsCreator, block_on
    import splink.comparison_library as cl
    from splink.spark.linker import SparkAPI

    id_col = "_row_id"

    # Prepara lado esquerdo (pendentes): adiciona ID único e normaliza nomes
    esquerda = (df_pendente
                .withColumn(id_col, F.monotonically_increasing_id())
                .withColumn("nome_normalizado_splink",
                            F.udf(normalizar_nome, StringType())(F.col(coluna_nome)))
                .select(id_col, "nome_normalizado_splink"))

    # Prepara lado direito (candidatos): normaliza nomes e cria ID
    direita = (df_candidatos
               .withColumn("nome_normalizado_splink",
                           F.udf(normalizar_nome, StringType())(F.col(coluna_candidato_nome)))
               .select(F.col(coluna_candidato_id).alias("cnpj_candidato"),
                       "nome_normalizado_splink")
               .withColumn(id_col, F.concat(F.lit("cand_"), F.col("cnpj_candidato"))))

    # Configuração do Splink: link_only (sem clusters), blocking pela primeira letra
    settings = SettingsCreator(
        link_type="link_only",
        blocking_rules_to_generate_predictions=[
            block_on("substr(nome_normalizado_splink, 1, 1)"),
        ],
        comparisons=[
            cl.JaroWinklerAtThresholds("nome_normalizado_splink", [0.9, 0.7]),
        ],
        retain_matching_columns=True,
        retain_intermediate_calculation_columns=False,
    )

    # Cria o linker com backend Spark
    db_api = SparkAPI(spark_session=spark)
    linker = Linker(
        [esquerda.withColumn("source_dataset", F.lit("pendente")),
         direita.withColumn("source_dataset", F.lit("candidatos"))],
        settings,
        db_api=db_api,
        input_table_aliases=["pendente", "candidatos"],
    )

    # Estima parâmetros (weights) via amostragem aleatória
    linker.training.estimate_u_using_random_sampling(max_pairs=1e6)

    # Gera predições (probabilidades de match)
    predicoes = linker.inference.predict(
        threshold_match_probability=SPLINK_MATCH_PROBABILITY_THRESHOLD
    )
    pred_spark_df = predicoes.as_spark_dataframe()

    # Para cada linha pendente, fica apenas com o match de maior probabilidade
    from pyspark.sql.window import Window
    w = Window.partitionBy(f"{id_col}_l").orderBy(F.col("match_probability").desc())
    melhores = (pred_spark_df
                .withColumn("rn", F.row_number().over(w))
                .filter(F.col("rn") == 1)
                .select(F.col(f"{id_col}_l").alias(id_col),
                        F.col(f"{id_col}_r").alias("cnpj_splink_raw"),
                        F.col("match_probability").alias("splink_match_probability"))
                .withColumn("cnpj_splink",
                            F.regexp_replace(F.col("cnpj_splink_raw"), "^cand_", ""))
                .drop("cnpj_splink_raw"))

    # Join com o DataFrame original para adicionar as colunas de resultado
    resultado = (df_pendente
                 .withColumn(id_col, F.monotonically_increasing_id())
                 .join(melhores, on=id_col, how="left")
                 .drop(id_col))
    return resultado
