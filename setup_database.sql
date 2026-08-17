-- Criação do banco e dos schemas usados pelas camadas do pipeline.
-- Nenhum tratamento de dados acontece aqui: apenas DDL de schema.

CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS trusted;
CREATE SCHEMA IF NOT EXISTS delivery;
