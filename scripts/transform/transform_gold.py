import os
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

def criar_spark_session():
    spark = (
        SparkSession.builder
        .appName("camada-gold-transform")
        .master("local[*]")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark
        
def ler_silver(spark, silver_path, data):
    dt = datetime.strptime(data, "%Y-%m-%d")
    caminho = os.path.join(
        silver_path,
        "cotacoes",
        f"year={dt.strftime('%Y')}",
        f"month={dt.strftime('%m')}",
        f"day={dt.strftime('%d')}",
    )  
    print(f"[INFO] Lendo Silver em: {caminho}")
    df = spark.read.parquet(caminho)
    return df

def calcular_metricas(df, data):
    window_alta = Window.orderBy(F.desc("change_pct"))
    df_gold = df.withColumn("rank_alta", F.rank().over(window_alta))
    window_queda = Window.orderBy(F.asc("change_pct"))
    df_gold = df_gold.withColumn("rank_queda", F.rank().over(window_queda))
    window_volume = Window.orderBy(F.desc("volume"))
    df_gold = df_gold.withColumn("rank_volume", F.rank().over(window_volume))
    df_gold = df_gold.withColumn(
        "variacao",
        F.when(F.col("change_pct") > 0, "alta")
        .otherwise("queda")
    ) 
    return df_gold

def salvar_gold(df, gold_path, data):
    dt = datetime.strptime(data, "%Y-%m-%d")
    caminho = os.path.join(
        gold_path,
        "cotacoes",
        f"year={dt.strftime('%Y')}",
        f"month={dt.strftime('%m')}",
        f"day={dt.strftime('%d')}",
    )
    print(f"[INFO] Salvando Gold em: {caminho}")
    df.write.mode("overwrite").parquet(caminho)
    print(f"[INFO] Gold salva com sucesso!")

def main():
    print("[START] Iniciando transformação Gold...")
    hoje = datetime.now().strftime("%Y-%m-%d")
    silver_path = "/opt/airflow/data/silver"
    gold_path = "/opt/airflow/data/gold"

    spark = criar_spark_session()
    df_silver = ler_silver(spark, silver_path, hoje)
    df_gold = calcular_metricas(df_silver, hoje)
    df_gold.show(5, truncate=False)
    salvar_gold(df_gold, gold_path, hoje)
    spark.stop()

    print("[DONE] Transformação Gold concluída com sucesso!")
    
if __name__ == "__main__":
    main()