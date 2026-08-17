"""
Resolução de CNPJ por nome usando **Splink** (record linkage probabilístico,
via modelo Fellegi-Sunter) rodando sobre o backend Spark do próprio Splink
(`splink.spark.SparkAPI`) — ou seja, a comparação continua sendo executada
pelo Spark, e não em pandas.

É a alternativa "estatística" ao fuzzy_match.py (que usa RapidFuzz,
comparação determinística por score de similaridade de string). Splink
treina os pesos de comparação (via Expectation-Maximization) a partir dos
próprios dados e retorna probabilidades de match, o que tende a ser mais
robusto quando há muitos nomes parecidos entre si (ex.: "Banco X S.A." vs
"Banco X — filial").

Uso: chamado por 02_trusted.py quando MATCH_STRATEGY=splink.
"""
import re
import unicodedata

from pyspark.sql import functions as F
from pyspark.sql.types import StringType, IntegerType

SPLINK_MATCH_PROBABILITY_THRESHOLD = 0.85


def normalizar_nome(nome: str) -> str:
    if nome is None:
        return ""
    nome = str(nome).upper().strip()
    nome = unicodedata.normalize("NFKD", nome).encode("ASCII", "ignore").decode("ASCII")
    nome = re.sub(r"[^A-Z0-9 ]", " ", nome)
    nome = re.sub(r"\s+", " ", nome).strip()
    return nome


def splink_match_dataframe(df_pendente, coluna_nome: str, df_candidatos, coluna_candidato_nome: str,
                            coluna_candidato_id: str, spark):
    """
    df_pendente: Spark DataFrame com nomes (coluna `coluna_nome`) ainda sem
        CNPJ resolvido (ex.: reclamações de bancos "conglomerado").
    df_candidatos: Spark DataFrame com a base de referência (bancos
        tratados), contendo `coluna_candidato_nome` e `coluna_candidato_id`
        (o CNPJ).
    Retorna df_pendente com duas colunas extras:
        cnpj_splink (string), splink_match_probability (double)
    """
    from splink import Linker, SettingsCreator, block_on
    import splink.comparison_library as cl
    from splink.spark.linker import SparkAPI

    id_col = "_row_id"

    esquerda = (
        df_pendente.withColumn(id_col, F.monotonically_increasing_id())
        .withColumn("nome_normalizado_splink", F.udf(normalizar_nome, StringType())(F.col(coluna_nome)))
        .select(id_col, "nome_normalizado_splink")
    )
    direita = (
        df_candidatos.withColumn("nome_normalizado_splink", F.udf(normalizar_nome, StringType())(F.col(coluna_candidato_nome)))
        .select(F.col(coluna_candidato_id).alias("cnpj_candidato"), "nome_normalizado_splink")
        .withColumn(id_col, F.concat(F.lit("cand_"), F.col("cnpj_candidato")))
    )

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

    db_api = SparkAPI(spark_session=spark)
    linker = Linker(
        [esquerda.withColumn("source_dataset", F.lit("pendente")),
         direita.withColumn("source_dataset", F.lit("candidatos"))],
        settings, db_api=db_api,
        input_table_aliases=["pendente", "candidatos"],
    )

    linker.training.estimate_u_using_random_sampling(max_pairs=1e6)
    predicoes = linker.inference.predict(threshold_match_probability=SPLINK_MATCH_PROBABILITY_THRESHOLD)
    pred_spark_df = predicoes.as_spark_dataframe()

    # fica só com o melhor match (maior probabilidade) por linha pendente
    from pyspark.sql.window import Window
    w = Window.partitionBy(f"{id_col}_l").orderBy(F.col("match_probability").desc())
    melhores = (
        pred_spark_df.withColumn("rn", F.row_number().over(w))
        .filter(F.col("rn") == 1)
        .select(
            F.col(f"{id_col}_l").alias(id_col),
            F.col(f"{id_col}_r").alias("cnpj_splink_raw"),
            F.col("match_probability").alias("splink_match_probability"),
        )
        .withColumn("cnpj_splink", F.regexp_replace(F.col("cnpj_splink_raw"), "^cand_", ""))
        .drop("cnpj_splink_raw")
    )

    resultado = (
        df_pendente.withColumn(id_col, F.monotonically_increasing_id())
        .join(melhores, on=id_col, how="left")
        .drop(id_col)
    )
    return resultado
