# Databricks notebook source
# MAGIC %md 
# MAGIC ## Importação de funções de apoio

# COMMAND ----------

from pyspark.sql.functions import explode, sequence, to_date

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
# MAGIC ## Realizando a criação da lógica para DM_CALENDARIO

# COMMAND ----------

#Criando uma view temporária com o range de datas informado. 
data_inicial = '1988-01-01'
data_final = '2100-12-31'

(
  spark.sql(f"select explode(sequence(to_date('{data_inicial}'), to_date('{data_final}'), interval 1 day)) as DATA")
    .createOrReplaceTempView('VW_DM_CALENDARIO')
)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW VW_DM_CALENDARIO_FULL AS
# MAGIC select
# MAGIC   year(data) * 10000 + month(DATA) * 100 + day(DATA) as NR_DATA,
# MAGIC   DATA,
# MAGIC   year(DATA) AS NR_ANO,
# MAGIC   date_format(DATA, 'MMMM') as NM_MES,
# MAGIC   month(DATA) as NO_MES,
# MAGIC   date_format(DATA, 'EEEE') as NM_DIA_SEMANA,
# MAGIC   dayofweek(DATA) AS NR_DIA_SEMANA,
# MAGIC   case
# MAGIC     when weekday(DATA) < 5 then 'S'
# MAGIC     else 'N'
# MAGIC   end as DS_DIA_UTIL,
# MAGIC   dayofmonth(DATA) as NR_DIA_MES,
# MAGIC   case
# MAGIC     when DATA = last_day(DATA) then 'S'
# MAGIC     else 'N'
# MAGIC   end as DS_ULTIMO_DIA_MES,
# MAGIC   dayofyear(DATA) as NR_DIA_ANO,
# MAGIC   weekofyear(DATA) as NR_SEMANA,
# MAGIC   quarter(DATA) as NR_TRIMESTRE
# MAGIC 
# MAGIC  
# MAGIC from
# MAGIC   VW_DM_CALENDARIO
# MAGIC order by
# MAGIC   DATA

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from vw_dm_calendario_full

# COMMAND ----------

df_dm_calendario = spark.sql("select * from vw_dm_calendario_full")

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC ## Gravando em camada gold

# COMMAND ----------

df_dm_calendario.write.partitionBy("NR_ANO").mode("overwrite").parquet(path_gold_dm + "dm_calendario")

# COMMAND ----------

