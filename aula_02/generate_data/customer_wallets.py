import random
from faker import Faker
import pandas as pd
from datetime import datetime

# Inicializar Faker
fake = Faker()

# Configurações
start_date = datetime(2025, 1, 1)
end_date = datetime(2025, 1, 28)

# Função para gerar dados para a tabela `customer_wallets`
def generate_customer_wallets(n_customers, output_file):
    customers = []
    for _ in range(n_customers):
        customers.append({
            "customer_id": fake.uuid4(),
            "name": fake.name(),
            "email": fake.email(),
            "btc_balance": round(random.uniform(0, 5), 6),  # Saldo em Bitcoin
            "usd_balance": round(random.uniform(1000, 50000), 2),  # Saldo em USD
            "last_update": fake.date_time_between(start_date=start_date, end_date=end_date).strftime('%Y-%m-%d %H:%M:%S')  # Última atualização
        })
    # Converter em DataFrame e salvar em CSV
    df = pd.DataFrame(customers)
    df.to_csv(output_file, index=False)
    print(f"Dados de `customer_wallets` salvos em: {output_file}")
    return df

# Executar geração e salvar como CSV
if __name__ == "__main__":
    n_customers = 10  # Número de clientes
    output_file = "customer_wallets.csv"  # Nome do arquivo CSV

    # Gerar e salvar
    generate_customer_wallets(n_customers, output_file)
