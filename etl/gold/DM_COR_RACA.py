# Databricks notebook source
# MAGIC %md
# MAGIC ## Arquivos e funções de apoio

# COMMAND ----------

# MAGIC %run /Eleicoes/utils/functions

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Paths utilizados

# COMMAND ----------

#paths
path_silver = "/FileStore/tables/eleicoes/silver/"
path_gold_dm = "/FileStore/tables/eleicoes/gold/dimensoes/"
path_gold_fatos = "/FileStore/tables/eleicoes/gold/fatos/"

#files
file_consulta_cand = "consulta_cand/"
file_bem_cand = "bem_candidato/"
files_list = [file_consulta_cand, file_bem_cand]

#variáveis para utilização da função fn_write_parquet
mode = "overwrite"

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Realizando leitura e criando DM_COR_RACA

# COMMAND ----------

df = spark.read.parquet(path_silver + file_consulta_cand).select("CD_COR_RACA", "DS_COR_RACA").distinct()
df.display()
df.printSchema()

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC ## Gravando em camada gold

# COMMAND ----------

fn_write_parquet(df, path_gold_dm, "dm_cor_raca", mode)

# COMMAND ----------

