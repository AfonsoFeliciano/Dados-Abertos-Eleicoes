# Databricks notebook source
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



# COMMAND ----------

# MAGIC %md 
# MAGIC ## Realizando leitura das tabelas

# COMMAND ----------

#Dimensoes
df_dm_calendario = spark.read.parquet(path_gold_dm + "dm_calendario")
df_dm_candidato = spark.read.parquet(path_gold_dm + "dm_candidato")
df_dm_cargo = spark.read.parquet(path_gold_dm + "dm_cargo")
df_dm_coligacao = spark.read.parquet(path_gold_dm + "dm_coligacao")
df_dm_cor_raca = spark.read.parquet(path_gold_dm + "dm_cor_raca")
df_dm_eleicao = spark.read.parquet(path_gold_dm + "dm_eleicao")
df_dm_estado_civil = spark.read.parquet(path_gold_dm + "dm_estado_civil")
df_dm_federacao = spark.read.parquet(path_gold_dm + "dm_federacao")
df_dm_genero = spark.read.parquet(path_gold_dm + "dm_genero")
df_dm_grau_instrucao = spark.read.parquet(path_gold_dm + "dm_grau_instrucao")
df_dm_nacionalidade = spark.read.parquet(path_gold_dm + "dm_nacionalidade")
df_dm_ocupacao = spark.read.parquet(path_gold_dm + "dm_ocupacao")
df_dm_partido = spark.read.parquet(path_gold_dm + "dm_partido")
df_dm_situacao_candidatura = spark.read.parquet(path_gold_dm + "dm_situacao_candidatura")
df_dm_tipo_bem_candidato = spark.read.parquet(path_gold_dm + "dm_tipo_bem_candidato")

#Fatos
df_fato_despesas = spark.read.parquet(path_gold_fatos + "fato_despesas")
df_fato_patrimonio_candidato = spark.read.parquet(path_gold_fatos + "fato_patrimonio_candidato")

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Criando views temporárias para análises

# COMMAND ----------

#Dimensoes
df_dm_calendario.createOrReplaceTempView("vw_dm_calendario")
df_dm_candidato.createOrReplaceTempView("vw_dm_candidato")
df_dm_cargo.createOrReplaceTempView("vw_dm_cargo")
df_dm_coligacao.createOrReplaceTempView("vw_dm_coligacao")
df_dm_cor_raca.createOrReplaceTempView("vw_dm_cor_raca")
df_dm_eleicao.createOrReplaceTempView("vw_dm_eleicao")
df_dm_estado_civil.createOrReplaceTempView("vw_dm_estado_civil")
df_dm_federacao.createOrReplaceTempView("vw_dm_federacao")
df_dm_genero.createOrReplaceTempView("vw_dm_genero")
df_dm_grau_instrucao.createOrReplaceTempView("vw_dm_grau_instrucao")
df_dm_nacionalidade.createOrReplaceTempView("vw_dm_nacionalidade")
df_dm_ocupacao.createOrReplaceTempView("vw_dm_ocupacao")
df_dm_partido.createOrReplaceTempView("vw_dm_partido")
df_dm_situacao_candidatura.createOrReplaceTempView("vw_dm_situacao_candidatura")
df_dm_tipo_bem_candidato.createOrReplaceTempView("vw_dm_tipo_bem_candidato")

#Fatos
df_fato_despesas.createOrReplaceTempView("vw_fato_despesas")
df_fato_patrimonio_candidato.createOrReplaceTempView("vw_fato_patrimonio_candidato")

# COMMAND ----------

df_dm_partido.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Query inicial com fatos e dimensões

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT 
# MAGIC 
# MAGIC DC.NM_CANDIDATO, 
# MAGIC DC.NR_CANDIDATO, 
# MAGIC DE.DS_ELEICAO,
# MAGIC DC.DS_CARGO,
# MAGIC GI.DS_GRAU_INSTRUCAO,
# MAGIC DG.DS_GENERO,
# MAGIC DO.DS_OCUPACAO,
# MAGIC DP.SG_PARTIDO,
# MAGIC DP.NM_PARTIDO,
# MAGIC SUM(FPC.VR_BEM_CANDIDATO) TOTAL_PATRIMONIO
# MAGIC 
# MAGIC FROM VW_FATO_PATRIMONIO_CANDIDATO FPC
# MAGIC 
# MAGIC LEFT JOIN VW_DM_CANDIDATO DC
# MAGIC   ON FPC.SQ_CANDIDATO = DC.SQ_CANDIDATO
# MAGIC   
# MAGIC LEFT JOIN VW_FATO_DESPESAS FD
# MAGIC   ON DC.SQ_CANDIDATO = FD.SQ_CANDIDATO
# MAGIC   
# MAGIC LEFT JOIN VW_DM_ELEICAO DE
# MAGIC   ON FD.CD_ELEICAO = DE.CD_ELEICAO
# MAGIC   
# MAGIC LEFT JOIN VW_DM_CARGO DC
# MAGIC   ON FD.CD_CARGO = DC.CD_CARGO
# MAGIC   
# MAGIC LEFT JOIN VW_DM_GRAU_INSTRUCAO GI
# MAGIC   ON FD.CD_GRAU_INSTRUCAO = GI.CD_GRAU_INSTRUCAO
# MAGIC   
# MAGIC LEFT JOIN VW_DM_GENERO DG
# MAGIC   ON FD.CD_GENERO = DG.CD_GENERO
# MAGIC   
# MAGIC LEFT JOIN VW_DM_OCUPACAO DO
# MAGIC   ON FD.CD_OCUPACAO = DO.CD_OCUPACAO
# MAGIC   
# MAGIC LEFT JOIN VW_DM_PARTIDO DP
# MAGIC   ON FD.NR_PARTIDO = DP.NR_PARTIDO
# MAGIC   
# MAGIC GROUP BY 
# MAGIC 
# MAGIC DC.NM_CANDIDATO, 
# MAGIC DC.NR_CANDIDATO, 
# MAGIC DE.DS_ELEICAO,
# MAGIC DC.DS_CARGO,
# MAGIC GI.DS_GRAU_INSTRUCAO,
# MAGIC DG.DS_GENERO,
# MAGIC DO.DS_OCUPACAO,
# MAGIC DP.SG_PARTIDO,
# MAGIC DP.NM_PARTIDO

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT 
# MAGIC 
# MAGIC DC.NM_CANDIDATO, 
# MAGIC DC.NR_CANDIDATO, 
# MAGIC DE.DS_ELEICAO,
# MAGIC DC.DS_CARGO,
# MAGIC GI.DS_GRAU_INSTRUCAO,
# MAGIC DG.DS_GENERO,
# MAGIC DO.DS_OCUPACAO,
# MAGIC DP.SG_PARTIDO,
# MAGIC DP.NM_PARTIDO,
# MAGIC SUM(FPC.VR_BEM_CANDIDATO) TOTAL_PATRIMONIO
# MAGIC 
# MAGIC FROM VW_FATO_PATRIMONIO_CANDIDATO FPC
# MAGIC 
# MAGIC LEFT JOIN VW_DM_CANDIDATO DC
# MAGIC   ON FPC.SQ_CANDIDATO = DC.SQ_CANDIDATO
# MAGIC   
# MAGIC LEFT JOIN VW_FATO_DESPESAS FD
# MAGIC   ON DC.SQ_CANDIDATO = FD.SQ_CANDIDATO
# MAGIC   
# MAGIC LEFT JOIN VW_DM_ELEICAO DE
# MAGIC   ON FD.CD_ELEICAO = DE.CD_ELEICAO
# MAGIC   
# MAGIC LEFT JOIN VW_DM_CARGO DC
# MAGIC   ON FD.CD_CARGO = DC.CD_CARGO
# MAGIC   
# MAGIC LEFT JOIN VW_DM_GRAU_INSTRUCAO GI
# MAGIC   ON FD.CD_GRAU_INSTRUCAO = GI.CD_GRAU_INSTRUCAO
# MAGIC   
# MAGIC LEFT JOIN VW_DM_GENERO DG
# MAGIC   ON FD.CD_GENERO = DG.CD_GENERO
# MAGIC   
# MAGIC LEFT JOIN VW_DM_OCUPACAO DO
# MAGIC   ON FD.CD_OCUPACAO = DO.CD_OCUPACAO
# MAGIC   
# MAGIC LEFT JOIN VW_DM_PARTIDO DP
# MAGIC   ON FD.NR_PARTIDO = DP.NR_PARTIDO
# MAGIC   
# MAGIC GROUP BY 
# MAGIC 
# MAGIC DC.NM_CANDIDATO, 
# MAGIC DC.NR_CANDIDATO, 
# MAGIC DE.DS_ELEICAO,
# MAGIC DC.DS_CARGO,
# MAGIC GI.DS_GRAU_INSTRUCAO,
# MAGIC DG.DS_GENERO,
# MAGIC DO.DS_OCUPACAO,
# MAGIC DP.SG_PARTIDO,
# MAGIC DP.NM_PARTIDO

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT 
# MAGIC 
# MAGIC DC.NM_CANDIDATO, 
# MAGIC DC.NR_CANDIDATO, 
# MAGIC DE.DS_ELEICAO,
# MAGIC DC.DS_CARGO,
# MAGIC GI.DS_GRAU_INSTRUCAO,
# MAGIC DG.DS_GENERO,
# MAGIC DO.DS_OCUPACAO,
# MAGIC DP.SG_PARTIDO,
# MAGIC DP.NM_PARTIDO,
# MAGIC SUM(FPC.VR_BEM_CANDIDATO) TOTAL_PATRIMONIO
# MAGIC 
# MAGIC FROM VW_FATO_PATRIMONIO_CANDIDATO FPC
# MAGIC 
# MAGIC LEFT JOIN VW_DM_CANDIDATO DC
# MAGIC   ON FPC.SQ_CANDIDATO = DC.SQ_CANDIDATO
# MAGIC   
# MAGIC LEFT JOIN VW_FATO_DESPESAS FD
# MAGIC   ON DC.SQ_CANDIDATO = FD.SQ_CANDIDATO
# MAGIC   
# MAGIC LEFT JOIN VW_DM_ELEICAO DE
# MAGIC   ON FD.CD_ELEICAO = DE.CD_ELEICAO
# MAGIC   
# MAGIC LEFT JOIN VW_DM_CARGO DC
# MAGIC   ON FD.CD_CARGO = DC.CD_CARGO
# MAGIC   
# MAGIC LEFT JOIN VW_DM_GRAU_INSTRUCAO GI
# MAGIC   ON FD.CD_GRAU_INSTRUCAO = GI.CD_GRAU_INSTRUCAO
# MAGIC   
# MAGIC LEFT JOIN VW_DM_GENERO DG
# MAGIC   ON FD.CD_GENERO = DG.CD_GENERO
# MAGIC   
# MAGIC LEFT JOIN VW_DM_OCUPACAO DO
# MAGIC   ON FD.CD_OCUPACAO = DO.CD_OCUPACAO
# MAGIC   
# MAGIC LEFT JOIN VW_DM_PARTIDO DP
# MAGIC   ON FD.NR_PARTIDO = DP.NR_PARTIDO
# MAGIC   
# MAGIC GROUP BY 
# MAGIC 
# MAGIC DC.NM_CANDIDATO, 
# MAGIC DC.NR_CANDIDATO, 
# MAGIC DE.DS_ELEICAO,
# MAGIC DC.DS_CARGO,
# MAGIC GI.DS_GRAU_INSTRUCAO,
# MAGIC DG.DS_GENERO,
# MAGIC DO.DS_OCUPACAO,
# MAGIC DP.SG_PARTIDO,
# MAGIC DP.NM_PARTIDO
# MAGIC 
# MAGIC ORDER BY SUM(FPC.VR_BEM_CANDIDATO) DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT 
# MAGIC 
# MAGIC DC.NM_CANDIDATO, 
# MAGIC DC.NR_CANDIDATO, 
# MAGIC DE.DS_ELEICAO,
# MAGIC DC.DS_CARGO,
# MAGIC GI.DS_GRAU_INSTRUCAO,
# MAGIC DG.DS_GENERO,
# MAGIC DO.DS_OCUPACAO,
# MAGIC DP.SG_PARTIDO,
# MAGIC DP.NM_PARTIDO,
# MAGIC SUM(FPC.VR_BEM_CANDIDATO) TOTAL_PATRIMONIO
# MAGIC 
# MAGIC FROM VW_FATO_PATRIMONIO_CANDIDATO FPC
# MAGIC 
# MAGIC LEFT JOIN VW_DM_CANDIDATO DC
# MAGIC   ON FPC.SQ_CANDIDATO = DC.SQ_CANDIDATO
# MAGIC   
# MAGIC LEFT JOIN VW_FATO_DESPESAS FD
# MAGIC   ON DC.SQ_CANDIDATO = FD.SQ_CANDIDATO
# MAGIC   
# MAGIC LEFT JOIN VW_DM_ELEICAO DE
# MAGIC   ON FD.CD_ELEICAO = DE.CD_ELEICAO
# MAGIC   
# MAGIC LEFT JOIN VW_DM_CARGO DC
# MAGIC   ON FD.CD_CARGO = DC.CD_CARGO
# MAGIC   
# MAGIC LEFT JOIN VW_DM_GRAU_INSTRUCAO GI
# MAGIC   ON FD.CD_GRAU_INSTRUCAO = GI.CD_GRAU_INSTRUCAO
# MAGIC   
# MAGIC LEFT JOIN VW_DM_GENERO DG
# MAGIC   ON FD.CD_GENERO = DG.CD_GENERO
# MAGIC   
# MAGIC LEFT JOIN VW_DM_OCUPACAO DO
# MAGIC   ON FD.CD_OCUPACAO = DO.CD_OCUPACAO
# MAGIC   
# MAGIC LEFT JOIN VW_DM_PARTIDO DP
# MAGIC   ON FD.NR_PARTIDO = DP.NR_PARTIDO
# MAGIC   
# MAGIC GROUP BY 
# MAGIC 
# MAGIC DC.NM_CANDIDATO, 
# MAGIC DC.NR_CANDIDATO, 
# MAGIC DE.DS_ELEICAO,
# MAGIC DC.DS_CARGO,
# MAGIC GI.DS_GRAU_INSTRUCAO,
# MAGIC DG.DS_GENERO,
# MAGIC DO.DS_OCUPACAO,
# MAGIC DP.SG_PARTIDO,
# MAGIC DP.NM_PARTIDO
# MAGIC 
# MAGIC ORDER BY SUM(FPC.VR_BEM_CANDIDATO) DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT 
# MAGIC 
# MAGIC DC.NM_CANDIDATO, 
# MAGIC DC.NR_CANDIDATO, 
# MAGIC DE.DS_ELEICAO,
# MAGIC DC.DS_CARGO,
# MAGIC GI.DS_GRAU_INSTRUCAO,
# MAGIC DG.DS_GENERO,
# MAGIC DO.DS_OCUPACAO,
# MAGIC DP.SG_PARTIDO,
# MAGIC DP.NM_PARTIDO,
# MAGIC SUM(FPC.VR_BEM_CANDIDATO) TOTAL_PATRIMONIO
# MAGIC 
# MAGIC FROM VW_FATO_PATRIMONIO_CANDIDATO FPC
# MAGIC 
# MAGIC LEFT JOIN VW_DM_CANDIDATO DC
# MAGIC   ON FPC.SQ_CANDIDATO = DC.SQ_CANDIDATO
# MAGIC   
# MAGIC LEFT JOIN VW_FATO_DESPESAS FD
# MAGIC   ON DC.SQ_CANDIDATO = FD.SQ_CANDIDATO
# MAGIC   
# MAGIC LEFT JOIN VW_DM_ELEICAO DE
# MAGIC   ON FD.CD_ELEICAO = DE.CD_ELEICAO
# MAGIC   
# MAGIC LEFT JOIN VW_DM_CARGO DC
# MAGIC   ON FD.CD_CARGO = DC.CD_CARGO
# MAGIC   
# MAGIC LEFT JOIN VW_DM_GRAU_INSTRUCAO GI
# MAGIC   ON FD.CD_GRAU_INSTRUCAO = GI.CD_GRAU_INSTRUCAO
# MAGIC   
# MAGIC LEFT JOIN VW_DM_GENERO DG
# MAGIC   ON FD.CD_GENERO = DG.CD_GENERO
# MAGIC   
# MAGIC LEFT JOIN VW_DM_OCUPACAO DO
# MAGIC   ON FD.CD_OCUPACAO = DO.CD_OCUPACAO
# MAGIC   
# MAGIC LEFT JOIN VW_DM_PARTIDO DP
# MAGIC   ON FD.NR_PARTIDO = DP.NR_PARTIDO
# MAGIC   
# MAGIC GROUP BY 
# MAGIC 
# MAGIC DC.NM_CANDIDATO, 
# MAGIC DC.NR_CANDIDATO, 
# MAGIC DE.DS_ELEICAO,
# MAGIC DC.DS_CARGO,
# MAGIC GI.DS_GRAU_INSTRUCAO,
# MAGIC DG.DS_GENERO,
# MAGIC DO.DS_OCUPACAO,
# MAGIC DP.SG_PARTIDO,
# MAGIC DP.NM_PARTIDO
# MAGIC 
# MAGIC ORDER BY SUM(FPC.VR_BEM_CANDIDATO) DESC