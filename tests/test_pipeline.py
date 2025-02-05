import pytest
import requests
from unittest.mock import patch, MagicMock
from datetime import datetime
from pyspark.sql import SparkSession
from main import fetch_bitcoin_price, save_to_table

# Inicializa a sessão Spark no Databricks (sem necessidade de criar localmente)
spark = SparkSession.builder.getOrCreate()

# Simulação de resposta da API Coinbase
mock_response = {
    "data": {
        "amount": "45000.00",
        "base": "BTC",
        "currency": "USD"
    }
}

@patch("requests.get")
def test_fetch_bitcoin_price_success(mock_get):
    """Testa se a API retorna os dados corretamente."""
    mock_get.return_value.status_code = 200
    mock_get.return_value.json.return_value = mock_response

    result = fetch_bitcoin_price()

    assert result["amount"] == 45000.00
    assert result["base"] == "BTC"
    assert result["currency"] == "USD"
    assert isinstance(result["datetime"], datetime)  # Confirma que há um timestamp

@patch("requests.get")
def test_fetch_bitcoin_price_api_error(mock_get):
    """Testa o comportamento da função quando a API retorna erro."""
    mock_get.side_effect = requests.exceptions.RequestException("Erro na API")

    with pytest.raises(Exception, match="Erro ao acessar a API"):
        fetch_bitcoin_price()

def test_save_to_table():
    """Testa a função save_to_table simulando a escrita no Databricks Delta Table."""
    test_data = {
        "amount": 45000.00,
        "base": "BTC",
        "currency": "USD",
        "datetime": datetime.now()
    }

    # Criar um mock para o DataFrame do Spark
    mock_df = spark.createDataFrame([test_data])

    with patch.object(mock_df, "write") as mock_write:
        save_to_table(test_data)
        
        # Verifica se o método `format("delta")` foi chamado
        mock_write.format.assert_called_with("delta")

        # Verifica se o método `mode("append")` foi chamado
        mock_write.format().mode.assert_called_with("append")

        # Verifica se o método `saveAsTable("bronze.bitcoin_price")` foi chamado
        mock_write.format().mode().saveAsTable.assert_called_with("bronze.bitcoin_price")
