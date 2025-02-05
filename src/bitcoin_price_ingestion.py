# Databricks notebook source
import requests
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType, TimestampType
from datetime import datetime

# Inicializar Spark
spark = SparkSession.builder.appName("Bitcoin Price Ingestion").getOrCreate()

# Configuração da API
API_URL = "https://api.coinbase.com/v2/prices/spot?currency=USD"

def fetch_bitcoin_price():
    """Obtém o preço atual do Bitcoin da API Coinbase."""
    try:
        response = requests.get(API_URL)
        response.raise_for_status()
        data = response.json()["data"]
        # Adiciona timestamp no formato datetime
        return {
            "amount": float(data["amount"]),
            "base": data["base"],
            "currency": data["currency"],
            "datetime": datetime.now()
        }
    except requests.exceptions.RequestException as e:
        raise Exception(f"Erro ao acessar a API: {e}")

def save_to_delta(data):
    """Salva os dados no Delta Lake."""
    # Definir o schema com a nova coluna datetime
    schema = StructType([
        StructField("amount", FloatType(), True),
        StructField("base", StringType(), True),
        StructField("currency", StringType(), True),
        StructField("datetime", TimestampType(), True)
    ])
    
    # Converter para DataFrame
    data_df = spark.createDataFrame([data], schema=schema)
    
    # Caminho no Delta Lake
    delta_path = "/mnt/delta/bitcoin_price"
    
    # Escrever no Delta Lake no modo append
    data_df.write.format("delta").mode("append").save(delta_path)
    print(f"Dados salvos em Delta Table no caminho: {delta_path}")

if __name__ == "__main__":
    # Obter dados da API
    print("Obtendo dados da API Coinbase...")
    bitcoin_price = fetch_bitcoin_price()
    print("Dados obtidos:", bitcoin_price)
    
    # Salvar no Delta Lake
    print("Salvando dados no Delta Lake...")
    save_to_delta(bitcoin_price)
    print("Pipeline concluído com sucesso.")

