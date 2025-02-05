import pytest
import requests
from unittest.mock import patch, MagicMock
from datetime import datetime
from pyspark.sql import SparkSession
import databricks.dbutils as dbutils  # Para chamar notebooks

# Inicializa a sessão Spark no Databricks
spark = SparkSession.builder.getOrCreate()

def test_fetch_bitcoin_price():
    """Testa se o notebook Bitcoin Price Ingestion retorna dados válidos."""
    result = dbutils.notebook.run("aula_04/bitcoin_price_ingestion", 60)  # Timeout de 60 segundos
    
    assert result is not None, "O notebook retornou None"
    
    data = eval(result)  # Converte string para dicionário (se necessário)
    
    assert isinstance(data, dict), "O resultado não é um dicionário"
    assert "amount" in data, "Campo 'amount' ausente"
    assert "base" in data, "Campo 'base' ausente"
    assert "currency" in data, "Campo 'currency' ausente"
    assert "datetime" in data, "Campo 'datetime' ausente"
    assert isinstance(data["datetime"], str), "O campo 'datetime' deve ser uma string"

def test_save_to_table():
    """Testa se o notebook Bitcoin Price Ingestion salva os dados corretamente."""
    test_data = {
        "amount": 45000.00,
        "base": "BTC",
        "currency": "USD",
        "datetime": str(datetime.now())
    }

    result = dbutils.notebook.run("aula_04/bitcoin_price_ingestion", 60, {"test_mode": "true"})

    assert result == "success", "Erro ao salvar dados na Delta Table"
