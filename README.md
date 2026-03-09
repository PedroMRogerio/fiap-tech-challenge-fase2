# FIAP Tech Challenge - Fase 2
## 📊 Pipeline Batch Bovespa — Tech Challenge FIAP

Pipeline de dados em batch para ingestão, processamento e análise de dados de ações da B3, utilizando serviços da AWS. O projeto implementa um fluxo completo de Data Engineering, desde a extração de dados financeiros até a consulta analítica via SQL.

## 🏗 Arquitetura da Solução

Python Extract → S3 Raw → Lambda → Glue ETL → S3 Refined → Athena

## 🎯 Objetivo do Projeto

Construir um pipeline de dados automatizado capaz de:
* Extrair dados de ações da B3
* Armazenar dados brutos em AWS S3
* Processar e transformar dados via AWS Glue
* Automatizar o pipeline com AWS Lambda
* Disponibilizar dados para análise via AWS Athena

## ⚙️ Tecnologias Utilizadas

- Python
- Pandas
- yfinance
- AWS S3
- AWS Lambda
- AWS Glue
- AWS Glue Catalog
- AWS Athena
- Apache Parquet
  
### 📥 Extração de Dados (Extract)

Os dados das ações são extraídos utilizando a biblioteca yfinance.

**Script de extração:**
```
extract_to_parquet.py
```
**Exemplo de execução:**
```
python extract_to_parquet.py --tickers PETR4.SA VALE3.SA ITUB4.SA
```
**Exemplo de saída**

Arquivo gerado:
```
b3_raw.parquet
```
**Schema:**

| Column | Type |
| ------ |:----:|
| Date | string |
| ticker | string |
| Open | float |
| High | float |
| Low	| float |
| Close |	float |
| Volume | int |

### 📦 Ingestão no Data Lake (RAW)

Os dados brutos são armazenados no Amazon S3.

**Formato:**

Parquet

**Estrutura:**
```
s3://fiap-fase2-tech-challenge/raw/

raw/
├── date=2026-03-01/
│     └── b3_raw.parquet
├── date=2026-03-02/
│     └── b3_raw.parquet
```
Particionamento diário.

### :robot: Automação com AWS Lambda

O bucket S3 possui um Event Notification que dispara uma função Lambda quando novos arquivos são inseridos.

Evento monitorado:
```
ObjectCreated
```
Prefixo:
```
raw/
```
A Lambda executa o Glue Job iniciando automaticamente o processo de ETL.

### :repeat: Processamento de Dados (ETL)

O processamento é realizado via AWS Glue Job (Spark).

Transformações aplicadas:

**A — Agregação**

Cálculo de métricas diárias:

* soma de volume negociado
* média de preço
* máximo
* mínimo

**B — Renomeação de colunas**
```
Close → close_price
Volume → volume_traded
```
**C — Cálculo baseado em data**

Cálculo de média móvel de 7 dias:
```
ma7_close
```

### :open_file_folder: Camada Refinada (Refined)

Os dados transformados são salvos em:
```
s3://fiap-fase2-tech-challenge/refined/
```
Formato:

Parquet

Estrutura:
```
refined/

date=2026-03-02/
   ticker=PETR4.SA/
       part-000.parquet

   ticker=VALE3.SA/
       part-000.parquet
```
Particionamento por:
```
date
ticker
```
### :books: Glue Catalog

O Glue Job registra automaticamente os dados no AWS Glue Catalog.

Tabela criada no database:
```
default
```
Tabela exemplo:
```
refined
```
Isso permite que os dados sejam consultados via Athena.

### :mag_right: Consulta de Dados (Athena)

Consulta exemplo para listar tickers disponíveis:
```
SELECT DISTINCT ticker
FROM b3_refined
ORDER BY ticker;
```
Consulta exemplo para análise de preços:
```
SELECT
    ticker,
    date,
    close_price,
    ma7_close
FROM b3_refined
ORDER BY date DESC
LIMIT 20;
```
### :chart_with_upwards_trend: Exemplo de Resultado
| ticker | date | close_price | ma7_close |
| ------ | ---- | ----------- | :-------: |
| PETR4.SA | 2026-03-02 | 37.10 | 36.82 |
| VALE3.SA | 2026-03-02 | 61.45 |	60.91 |

### :repeat: Fluxo Automatizado

Pipeline completo:
```
Upload parquet → S3 RAW
       ↓
Trigger S3
       ↓
Lambda
       ↓
Glue Job
       ↓
S3 Refined
       ?
Glue Catalog
       ↓
Athena
```
Todo o pipeline ocorre automaticamente após ingestão de novos dados.
