# Atividade 3 — Ingestão e ETL com Python + Spark

- **Antonio Daniel de Souza Linhares**
- **Yuri Alexandre Barbosa Rodrigues**
- **Hercules Ramos Veloso de Freitas**



## Bases de origem

| Base        | Arquivo(s)                                                                     | Formato               |
| ----------- | ------------------------------------------------------------------------------ | ---------------------- |
| Reclamações | `Reclamações/*.csv` (8 arquivos trimestrais, 2021-2022)                        | `;` , Latin-1          |
| Bancos      | `Bancos/EnquadramentoInicia_v2.tsv`                                            | `\t` , Latin-1         |
| Empregados  | `Empregados/glassdoor_consolidado_join_match_v2.csv` e `..._match_less_v2.csv` | `\|` , UTF-8           |

Coloque os arquivos originais em `data/raw/origem/` (mesma estrutura de
pastas dos repositórios anteriores: `Reclamações/`, `Bancos/`,
`Empregados/`) antes de rodar o pipeline.

## Arquitetura das camadas

```
data/raw/origem/            <- arquivos originais, sem nenhuma alteração
     |
     v  (Spark — leitura com encoding/delimitador corretos, tudo como string)
data/raw/*.parquet          <- camada RAW em disco (espelho da fonte)
schema "raw" (Postgres)     <- camada RAW no banco (tabela por fonte)
     |
     v  (Spark DataFrame API — limpeza, tipagem, join de chaves, dedup,
     v   fuzzy matching de nomes)
data/trusted/*.parquet      <- 3 tabelas tratadas, em Parquet
schema "trusted" (Postgres) <- as mesmas 3 tabelas tratadas, no banco
     |
     v  (Spark DataFrame API — join final pela chave CNPJ)
data/delivery/*.parquet     <- tabela final, tratada e unida, em Parquet
schema "delivery" (Postgres)<- delivery.tb_reclamacoes_bancos_funcionarios
```

Etapas (`src/`):

| Script            | Faz o quê                                                                                   |
| ------------------ | --------------------------------------------------------------------------------------------- |
| `01_ingest_raw.py`  | Lê as fontes originais via Spark (`spark.read.csv`), grava em Parquet e no schema `raw`.       |
| `02_trusted.py`     | Tipagem, normalização de CNPJ, dedup, resolução de CNPJ por nome (exato + matching aproximado — fuzzy **ou** splink, ver abaixo). Grava em Parquet e no schema `trusted`. |
| `03_delivery.py`    | Join final das 3 tabelas Trusted pela chave `cnpj`. Grava em Parquet e no schema `delivery`.   |
| `fuzzy_match.py`    | Estratégia **fuzzy**: RapidFuzz (`token_sort_ratio`) distribuído via `DataFrame.mapInPandas` — o motor de execução continua sendo o Spark. |
| `splink_match.py`   | Estratégia **splink**: matching probabilístico (Fellegi-Sunter, com EM) via [Splink](https://moj-analytical-services.github.io/splink/), usando o backend Spark do próprio Splink (`SparkAPI`). |
| `run_all.py`        | Aguarda o Postgres subir e roda as 3 etapas em sequência, repassando a flag de matching.        |

### Escolhendo a estratégia de matching (fuzzy vs. splink)

A resolução de CNPJ dos bancos "conglomerado" (por nome, quando o
cruzamento exato falha) pode usar **uma das duas estratégias**, escolhida
por flag — nunca as duas ao mesmo tempo:

| Estratégia | Como funciona | Quando usar |
| ---------- | -------------- | ------------ |
| `fuzzy` (default) | RapidFuzz, similaridade de string (`token_sort_ratio`), aceita match automático com score ≥ 88. Rápido e determinístico. | Poucos milhares de nomes, regra simples, resultado fácil de auditar. |
| `splink` | Record linkage probabilístico (Fellegi-Sunter): treina pesos de comparação via Expectation-Maximization e retorna uma *probabilidade* de match (aceita ≥ 0.85). | Quando há muita ambiguidade entre nomes parecidos e vale a pena um modelo estatístico em vez de um único score de string. |

A escolha é feita por variável de ambiente **ou** argumento de linha de
comando (o argumento tem prioridade sobre a variável de ambiente):

```bash
# via variável de ambiente
MATCH_STRATEGY=fuzzy  python3 src/02_trusted.py
MATCH_STRATEGY=splink python3 src/02_trusted.py

# via flag, incluindo através do run_all.py
python3 src/run_all.py --match-strategy splink

# via docker compose
MATCH_STRATEGY=splink docker compose up --build
```

A coluna `cnpj_origem` na tabela `trusted.reclamacoes` registra qual
método resolveu cada linha (`"match exato"`, `"fuzzy match, score=N"`,
`"splink match, prob=0.NNN"` ou `"nao resolvido"`), e a coluna
`match_strategy_usada` guarda qual das duas flags foi usada na rodada.
Cada execução também grava uma tabela extra
`trusted.reclamacoes_fuzzy` ou `trusted.reclamacoes_splink` (sem
sobrescrever uma à outra), para permitir comparar os dois resultados lado
a lado.

## Principais decisões de tratamento

Herdadas da Atividade 2 (mesmas regras de negócio, agora expressas com
`pyspark.sql.functions` em vez de pandas):

1. **Chave de integração = CNPJ raiz (8 dígitos)**, normalizado com
   `regexp_replace` + `lpad` para eliminar a diferença de zero-padding
   entre as bases de Reclamações e as demais.
2. **CNPJ dos bancos "conglomerado"** (Bradesco, Itaú, Santander, BB,
   Caixa, BTG etc.) é resolvido pelo nome: primeiro por match exato de
   nome normalizado (`normalizar_nome`, `pyspark.sql.functions.udf`),
   depois por fuzzy matching (RapidFuzz, score mínimo 88) para os casos
   restantes. O resultado fica registrado em `cnpj_origem`
   ("match exato" / "fuzzy match, score=N" / "nao resolvido").
3. **Deduplicação de Bancos por CNPJ** usando `Window` + `row_number()`,
   priorizando o nome "- PRUDENCIAL" e preservando o outro nome em
   `nome_alternativo`.
4. **Empregados (Glassdoor)**: os dois arquivos são unidos
   (`unionByName`) e deduplicados por CNPJ com `Window`, priorizando o
   registro do arquivo `match`.

## Como rodar

### Opção A — Docker Compose (recomendado)

Sobe o PostgreSQL e um container Python com PySpark rodando em modo
local (`local[*]`), e executa o pipeline completo:

```bash
docker compose up --build
```

### Opção B — manual (Python + Spark local)

```bash
# 1) instalar dependências (requer Java 8/11/17 instalado para o Spark)
pip install -r requirements.txt

# 2) subir o PostgreSQL e criar os schemas (uma vez só)
psql -U postgres -f setup_database.sql
# ajuste host/porta/usuário/senha via variáveis de ambiente
# DB_HOST / DB_PORT / DB_USER / DB_PASSWORD / DB_NAME, se necessário

# 3) rodar o pipeline completo
cd src
python3 run_all.py
```

Isso equivale a rodar, em sequência:
`01_ingest_raw.py` → `02_trusted.py` → `03_delivery.py`.

## Tabela final (Delivery)

`delivery.tb_reclamacoes_bancos_funcionarios` — granularidade **banco x
trimestre**, reunindo:

- indicadores de reclamações (índice, quantidade total) — BACEN
- segmento prudencial e nome oficial do banco — enquadramento
- avaliações de funcionários (Glassdoor), quando disponíveis
  (`possui_avaliacao_glassdoor`)

## Estrutura de arquivos

```
Dockerfile
docker-compose.yml
requirements.txt
setup_database.sql
data/
  raw/origem/            arquivos originais (entrada)
  raw/*.parquet           camada RAW em disco
  trusted/*.parquet       camada Trusted (Parquet)
  delivery/*.parquet      camada Delivery (Parquet)
src/
  spark_session.py         criação da SparkSession (com driver JDBC do Postgres)
  db.py                     leitura/escrita de tabelas no Postgres via JDBC
  01_ingest_raw.py          ingestão da camada RAW
  02_trusted.py             tratamento -> camada Trusted
  03_delivery.py            união -> camada Delivery (tabela final)
  fuzzy_match.py            fuzzy matching de nomes (RapidFuzz + Spark mapInPandas)
  run_all.py                aguarda o Postgres e executa as 3 etapas em sequência
```
