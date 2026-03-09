import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.window import Window

args = getResolvedOptions(sys.argv, ["JOB_NAME", "RAW_S3_PATH", "REFINED_S3_PATH"])
RAW_S3_PATH = args["RAW_S3_PATH"]
REFINED_S3_PATH = args["REFINED_S3_PATH"]

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

print("=== ETL B3 INICIO ===")
print("RAW_S3_PATH =", RAW_S3_PATH)
print("REFINED_S3_PATH =", REFINED_S3_PATH)

raw_df = spark.read.parquet(RAW_S3_PATH + "date=*/b3_raw.parquet")

print("RAW columns:", raw_df.columns)
raw_df.printSchema()

raw_count = raw_df.count()
print("raw_df_count =", raw_count)
raw_df.show(5, truncate=False)

if raw_count == 0:
    raise Exception("RAW vazio. Verifique se existem parquets em raw/date=YYYY-MM-DD/")

# Esperamos: Date, ticker, Open, High, Low, Close e Volume
expected = ["Date", "ticker", "Open", "High", "Low", "Close", "Volume"]
missing = [c for c in expected if c not in raw_df.columns]
if missing:
    raise Exception(f"Faltam colunas no raw: {missing}. Colunas disponíveis: {raw_df.columns}")

df = raw_df

# cria coluna date manualmente a partir de Date
df = df.withColumn("date", F.col("Date"))

# cria coluna de ordenação
df = df.withColumn("event_date", F.to_date(F.col("Date")))

# (B) Renomear 2 colunas
df = (df
      .withColumnRenamed("Close", "close_price")
      .withColumnRenamed("Volume", "volume_traded")
)

# (C) Média móvel 7 dias
w7 = Window.partitionBy("ticker").orderBy(F.col("event_date")).rowsBetween(-6, 0)
df = df.withColumn("ma7_close", F.avg(F.col("close_price")).over(w7))

# (A) Agregação por ticker e date
daily = (df.groupBy("ticker", "date")
           .agg(
               F.sum("volume_traded").alias("daily_volume_sum"),
               F.avg("close_price").alias("daily_close_avg"),
               F.max("close_price").alias("daily_close_max"),
               F.min("close_price").alias("daily_close_min")
           )
        )

out = df.join(daily, on=["ticker", "date"], how="left").dropDuplicates(["ticker", "event_date"])

out_count = out.count()
print("out_count =", out_count)
out.select("ticker","date","event_date","close_price","volume_traded","ma7_close","daily_volume_sum").show(20, truncate=False)

if out_count == 0:
    raise Exception("OUT vazio após transformações.")

(out
 .write
 .mode("append")
 .format("parquet")
 .partitionBy("date", "ticker")
 .save(REFINED_S3_PATH)
)

print("=== ETL B3 FIM (WRITE OK) ===")
job.commit()