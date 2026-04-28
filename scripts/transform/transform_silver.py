import os
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def criar_spark_session():
    spark = (
        SparkSession.builder
        .appName("b3-silver-transform")
        .master("local[*]")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark

def ler_bronze(spark, bronze_path, data):
    dt = datetime.strptime(data, "%Y-%m-%d")
    caminho = os.path.join(
        bronze_path,
        "cotacoes",
        f"year={dt.strftime('%Y')}",
        f"month={dt.strftime('%m')}",
        f"day={dt.strftime('%d')}",
    )
    print(f"[INFO] Lendo Bronze em: {caminho}")
    
    df_raw = spark.read.option("multiLine", "true").json(caminho)
    df = df_raw.select(F.explode("results").alias("data")).select("data.*") 
    print(f"[INFO] {df.count()} registros lidos da Bronze.")
    return df


def transformar(df, data):
    df_silver = (
        df
        .select(
            F.col("symbol"),
            F.col("longName").alias("name"),
            F.col("regularMarketPrice").alias("price"),
            F.col("regularMarketChange").alias("change"),
            F.col("regularMarketChangePercent").alias("change_pct"),
            F.col("regularMarketVolume").alias("volume"),
            F.col("marketCap").alias("market_cap"),
            F.col("regularMarketTime").alias("market_time_raw"),
        )
   .withColumn("market_time", F.to_timestamp(F.col("market_time_raw")))
        .drop("market_time_raw")
        .withColumn("ingestion_date", F.lit(data))
    ) 
    return df_silver

def salvar_silver(df, silver_path, data):
    # Transforma o texto "2026-04-22" num objeto de data
    # para poder extrair ano, mês e dia separados
    dt = datetime.strptime(data, "%Y-%m-%d")
    
    # Monta o caminho da pasta particionada
    # resultado: /opt/airflow/data/silver/cotacoes/year=2026/month=04/day=22/
    caminho = os.path.join(
        silver_path,          # /opt/airflow/data/silver
        "cotacoes",           # /cotacoes
        f"year={dt.strftime('%Y')}",   # /year=2026
        f"month={dt.strftime('%m')}",  # /month=04
        f"day={dt.strftime('%d')}",    # /day=22
    )
    
    # Mostra no log qual pasta está sendo usada
    print(f"[INFO] Salvando Silver em: {caminho}")
    
    # Salva os dados no formato Parquet
    # mode("overwrite") = se já existir arquivo nessa pasta, apaga e coloca o novo
    # .parquet(caminho) = salva no formato Parquet no caminho definido acima
    df.write.mode("overwrite").parquet(caminho)
    
    # Confirma no log que terminou com sucesso
    print(f"[INFO] Silver salva com sucesso!")
    
    
def main():
    print("[START] Iniciando transformação Silver...")
    hoje = datetime.now().strftime("%Y-%m-%d")
    bronze_path = "/opt/airflow/data/bronze"
    silver_path = "/opt/airflow/data/silver"

    spark = criar_spark_session()
    df_bronze = ler_bronze(spark, bronze_path, hoje)
    df_silver = transformar(df_bronze, hoje)
    df_silver.show(5, truncate=False)
    salvar_silver(df_silver, silver_path, hoje)
    spark.stop()

    print("[DONE] Transformação Silver concluída com sucesso!")
    
if __name__ == "__main__":
    main()