# Databricks notebook source
# MAGIC %md
# MAGIC ## Arquivos e funções de apoio

# COMMAND ----------

from pyspark.sql.functions import col, when, from_utc_timestamp, from_unixtime

# COMMAND ----------

# MAGIC %run /Eleicoes/utils/functions

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Paths utilizados

# COMMAND ----------

#paths
path_bronze = "/FileStore/tables/eleicoes/bronze/"
path_silver = "/FileStore/tables/eleicoes/silver/"

#files
file_consulta_cand = "consulta_cand/"
file_bem_cand = "bem_candidato/"
files_list = [file_consulta_cand]

#variáveis para utilização da função fn_write_parquet
mode = "overwrite"

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Leitura do arquivo

# COMMAND ----------

df = spark.read.parquet(path_bronze + file_consulta_cand)
df.display()

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Visualizando schema do arquivo

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Tratamentos 

# COMMAND ----------

# MAGIC %md 
# MAGIC ### 1 - Substituindo #NULO#, #NULO, #NE, #NE# por null/none em todas as colunas

# COMMAND ----------

df = fn_replace_values_with_null(df, "#NULO#")
df = fn_replace_values_with_null(df, "#NULO")
df = fn_replace_values_with_null(df, "#NE")
df = fn_replace_values_with_null(df, "#NE#")

# COMMAND ----------

# MAGIC %md 
# MAGIC ### 2 - Alterando tipagem das colunas

# COMMAND ----------

integer_columns =  [
                    'CD_TIPO_ELEICAO', 
                    'NR_TURNO', 
                    'CD_ELEICAO', 
                    'CD_CARGO', 
                    'NR_CANDIDATO', 
                    'CD_SITUACAO_CANDIDATURA', 
                    'CD_DETALHE_SITUACAO_CAND', 
                    'NR_PARTIDO', 
                    'NR_FEDERACAO', 
                    'CD_NACIONALIDADE', 
                    'CD_MUNICIPIO_NASCIMENTO', 
                    'NR_IDADE_DATA_POSSE', 
                    'CD_GENERO', 
                    'CD_GRAU_INSTRUCAO', 
                    'CD_ESTADO_CIVIL', 
                    'CD_COR_RACA', 
                    'CD_OCUPACAO', 
                    'CD_SIT_TOT_TURNO', 
                    'NR_PROTOCOLO_CANDIDATURA', 
                    'CD_SITUACAO_CANDIDATO_PLEITO', 
                    'CD_SITUACAO_CANDIDATO_URNA', 
                    'CD_SITUACAO_CANDIDATO_TOT'
                    ]


double_columns = ['VR_DESPESA_MAX_CAMPANHA']
    
df = fn_convert_multicolumn_type(df, integer_columns, 'integer')
df = fn_convert_multicolumn_type(df, double_columns, 'double')

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC ## Gravando em camada silver

# COMMAND ----------

fn_write_parquet(df, path_silver, file_consulta_cand, mode)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Valida a escrita

# COMMAND ----------

df = fn_validate_folders(files_list, path_silver)
df.display()
