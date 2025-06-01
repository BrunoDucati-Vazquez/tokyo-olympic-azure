# Projeto de Engenharia de Dados - Olimpíadas de Tóquio 2021

![Arquitetura da Solução](https://github.com/user-attachments/assets/1cd55077-70bf-4e96-bb1f-6d790f84cdeb)

## Visão Geral do Projeto
Este projeto de engenharia de dados processa e analisa informações das Olimpíadas de Tóquio 2021. A solução utiliza serviços da nuvem Azure para extrair, transformar e carregar (ETL) dados do Kaggle em um ambiente analítico, permitindo insights sobre desempenho de atletas, distribuição de medalhas e rankings de países.

## Arquitetura da Solução
A solução segue uma arquitetura moderna de pipeline de dados:
1. **Fonte de Dados**: Arquivos CSV brutos do Kaggle
2. **Camada de Ingestão**: Azure Data Factory para orquestração
3. **Camada de Armazenamento**: Azure Data Lake Gen2
4. **Camada de Processamento**: Databricks para transformações
5. **Saída**: Conjuntos de dados curados para análise

## Fontes de Dados
O projeto utiliza o dataset ["2021 Olympics in Tokyo" do Kaggle](https://www.kaggle.com/datasets/arjunprasadsarkhel/2021-olympics-in-tokyo), contendo 5 arquivos CSV:
- `Athletes.csv` - Informações sobre atletas
- `Coaches.csv` - Informações sobre técnicos
- `EntriesGender.csv` - Distribuição por gênero por disciplina
- `Medals.csv` - Quadro de medalhas por país
- `Teams.csv` - Informações sobre equipes

## Notebook

Foi Usado o Data Bricks para escrever código Python, aplicando transformações e escrever querys de consulta: https://github.com/BrunoDucati-Vazquez/tokyo-olympic-azure/blob/main/Tokyo%20Olympic%20Transformation.ipynb

## Fluxo de Processamento

### Pipeline no Data Factory
![Pipeline Data Factory](https://github.com/user-attachments/assets/3d1a1db3-8197-4524-b8da-c9199903d1d6)

O pipeline no Azure Data Factory realiza as seguintes operações:
1. Copia dados brutos da fonte para a zona "raw" no Data Lake
2. Dispara notebooks Databricks para transformação
3. Move dados processados para a zona "transformed"

### Estrutura no Data Lake
![Estrutura de Pastas](https://github.com/user-attachments/assets/07782b6c-8eea-49cf-81a0-9795bde814f4)

### Perguntas a serem responsidas pelas querys

1 - Quais países ganharam mais medalhas?
 
```
from pyspark.sql.functions import col, sum, desc

# Calculate total medals per country (Gold + Silver + Bronze)
medal_counts = medals.withColumn("TotalMedals", 
                    col("Gold") + col("Silver") + col("Bronze")) \
               .select("Team_Country", "Gold", "Silver", "Bronze", "TotalMedals") \
               .orderBy(desc("TotalMedals"))

medal_counts.show()
```
2 - Qual é a distribuição de gênero nos diferentes esportes?
```
from pyspark.sql.functions import round

gender_distribution = entriesgender.withColumn(
    "FemalePercentage", round((col("Female")/col("Total"))*100, 2)
).withColumn(
    "MalePercentage", round((col("Male")/col("Total"))*100, 2)
).select(
    "Discipline", "Female", "Male", "Total", 
    "FemalePercentage", "MalePercentage"
).orderBy("Discipline")

gender_distribution.show()
```
3 - Quantos atletas cada país enviou?
```
athletes_by_country = athletes.groupBy("Country") \
    .count() \
    .withColumnRenamed("count", "AthleteCount") \
    .orderBy(desc("AthleteCount"))

athletes_by_country.show()
```
4 - Qual é a proporção de atletas por medalha por país?
```
from pyspark.sql.functions import when

# First join athletes count with medals data
athlete_medal_ratio = athletes_by_country.join(
    medals, 
    athletes_by_country["Country"] == medals["Team_Country"],
    "left"
).select(
    athletes_by_country["Country"],
    "AthleteCount",
    col("Gold").alias("GoldMedals"),
    col("Silver").alias("SilverMedals"),
    col("Bronze").alias("BronzeMedals"),
    (col("Gold") + col("Silver") + col("Bronze")).alias("TotalMedals")
).withColumn(
    "AthleteToMedalRatio",
    when(col("TotalMedals") > 0, 
         round(col("AthleteCount")/(col("Gold") + col("Silver") + col("Bronze")), 2)
    .otherwise(None)
).orderBy("AthleteToMedalRatio")

athlete_medal_ratio.show()
```
5 - Quais esportes têm a participação de gênero mais equilibrada?
```
from pyspark.sql.functions import abs

most_balanced_sports = gender_distribution.withColumn(
    "GenderBalanceScore",
    abs(col("FemalePercentage") - col("MalePercentage"))
).select(
    "Discipline", 
    "FemalePercentage", 
    "MalePercentage", 
    "GenderBalanceScore"
).orderBy("GenderBalanceScore")

most_balanced_sports.show()
```
