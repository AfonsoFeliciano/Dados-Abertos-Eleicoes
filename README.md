# Dados Abertos Eleições
Repositório para processamento e modelagem dimensional dos dados das eleições utilizando Spark no Databricks Community


## Base de Dados

A base de dados e sua dicionarização se encontram no link: https://dados.gov.br/dataset/candidatos-2022


## Sobre as camadas

### ERP

A camada ERP é meramente ilustrativa, utilizada apenas para armazenar os arquivos em seu formato original no qual foram baixados do portal de dados abertos. 

### Bronze

Em camada bronze, os arquivos foram lidos e unidos e salvos em arquivo colunar, neste caso parquet com o objetivo de utilizar sua compressão.

### Silver

Na camada silver os dados foram limpados e realizados os mais diversos tratamentos para torná-los confiáveis para consumo.

### Gold

Por fim, em camada gold, os dados foram modelados utilizando modelagem dimensional, fisicalizando tabelas fatos e dimensões. 

## Modelagem

Abaixo são listadas as tabelas fatos e dimensões criadas durante a codificação do projeto.

### Fatos

- FATO_DESPESAS
- FATO_PATRIMONIO_CANDIDATO

### Dimensões

- DM_CALENDARIO
- DM_CANDIDATO
- DM_CARGO
- DM_COLIGACAO
- DM_COR_RACA
- DM_ELEICAO
- DM_ESTADO_CIVIL
- DM_FEDERACAO
- DM_GENERO
- DM_GRAU_INSTRUCAO
- DM_NACIONALIDADE
- DM_OCUPACAO
- DM_PARTIDO
- DM_SITUACAO_CANDIDATURA
- DM_TIPO_BEM_CANDIDATO


## Análises

A seguir, observa-se algumas perguntas que podem ser respondidas através do dados utilizados. 

- Qual o total de despesas por partido? 
- Qual o total de despesas por genêro? 
- Qual o total de despesas por coligação? 
- Qual o total de despesas por ano, mês? 
- Quais estados civis geram mais despesas durante a eleição? 
- Qual a quantidade total de candidatos por ano? 
- Qual a quantidade total de candidatos por partido? 
- Qual a quantidade total de candidatos por ocupação? 
- Qual a quantidade total de candidatos por genêro? 
- Qual a quantidade total de candidatos por federação? 
- Qual a quantidade total de candidatos por UF?
- Qual a quantidade total de bens por candidato? 
- Qual a quantidade total de bens por partido? 
- Qual a quantidade total de bens por genêro? 
- Qual a quantidade total de bens por grau de instrução? 
- Qual o patrimônio em bens por grau de instrução? 
- Qual o patrimônio em bens por genêro? 
- Qual o patrimônio em bens por ocupação? 
- Qual o patrimônio em bens por candidato? 

Além dessas, torna-se possível realizar a exploração e elaborar outras análises. 







