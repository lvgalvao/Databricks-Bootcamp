import random
from faker import Faker
import json
from datetime import datetime

# Inicializar Faker
fake = Faker()

# Configurações
start_date = datetime(2025, 1, 1)
end_date = datetime(2025, 1, 28)

# Função para gerar um único cliente como JSON
def generate_customer_json():
    customer = {
        "customer_id": fake.uuid4(),
        "name": fake.name(),
        "email": fake.email(),
        "btc_balance": round(random.uniform(0, 5), 6),  # Saldo em Bitcoin
        "usd_balance": round(random.uniform(1000, 50000), 2),  # Saldo em USD
        "last_update": fake.date_time_between(start_date=start_date, end_date=end_date).strftime('%Y-%m-%d %H:%M:%S')  # Última atualização
    }
    return customer

# Salvar cada cliente como um arquivo JSON localmente
def save_customers_to_local(n_customers, output_dir):
    for i in range(n_customers):
        customer = generate_customer_json()
        json_file_name = f"{output_dir}/customer_{i+1}.json"
        
        # Salvar localmente como arquivo JSON
        with open(json_file_name, "w") as json_file:
            json.dump(customer, json_file, indent=4)
        print(f"Arquivo {json_file_name} salvo localmente.")

# Executar geração e salvar localmente
if __name__ == "__main__":
    n_customers = 10  # Número de clientes
    output_dir = "."  # Diretório de saída

    # Gerar e salvar localmente
    save_customers_to_local(n_customers, output_dir)
