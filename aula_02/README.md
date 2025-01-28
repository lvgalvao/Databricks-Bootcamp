# Segunda aula

Nosso objetivo nessa primeira aula é configurar nosso ambiente no Databricks, ter uma visão da arquitetura geral do Spark (e do Databricks) e vamos ter um hands-on de código.

## Configuração Databricks e Github

## Ingestão da API

Ingerir dados da API da **Coinbase** que fornece o preço atual do Bitcoin. Esses dados devem ser salvos em uma **Delta Table** no Databricks a cada 15 minutos, garantindo consistência, escalabilidade e consulta eficiente no Delta Lake.

---

### **Código**

1. **Consumo da API:**
   - O código acessa a API da Coinbase (`https://api.coinbase.com/v2/prices/spot?currency=USD`) para obter o preço atual do Bitcoin.
   - Ele extrai os dados no formato JSON e converte para um formato estruturado no PySpark DataFrame.

```python
import requests
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType

# Inicializar Spark
spark = SparkSession.builder.appName("Bitcoin Price Integration").getOrCreate()

# Configurações
API_URL = "https://api.coinbase.com/v2/prices/spot?currency=USD"
DELTA_PATH = "/mnt/delta/bitcoin_price"

def fetch_bitcoin_price():
    """Obtém o preço atual do Bitcoin da API Coinbase."""
    try:
        response = requests.get(API_URL)
        response.raise_for_status()
        data = response.json()['data']
        return {
            "amount": float(data['amount']),
            "base": data['base'],
            "currency": data['currency']
        }
    except requests.exceptions.RequestException as e:
        raise Exception(f"Erro ao acessar a API: {e}")

def save_to_delta(data):
    """Salva os dados no Delta Lake."""
    # Schema para DataFrame
    schema = StructType([
        StructField("amount", FloatType(), True),
        StructField("base", StringType(), True),
        StructField("currency", StringType(), True)
    ])
    
    # Converter para DataFrame
    data_df = spark.createDataFrame([data], schema=schema)
    
    # Salvar no Delta Lake
    data_df.write.format("delta").mode("append").save(DELTA_PATH)
    print(f"Dados salvos em Delta Table no caminho: {DELTA_PATH}")

if __name__ == "__main__":
    print("Iniciando integração com API...")
    bitcoin_price = fetch_bitcoin_price()
    print(f"Preço do Bitcoin: {bitcoin_price['amount']} {bitcoin_price['currency']}")
    save_to_delta(bitcoin_price)
    print("Pipeline concluído com sucesso.")
```

## 2. **Escrita no Delta Lake:**
   - O dado extraído é salvo no **Delta Lake** no caminho `/mnt/delta/bitcoin_price`.
   - **Delta Lake** é usado para:
     - Garantir consistência (ACID transactions).
     - Registrar mudanças com logs transacionais (`_delta_log`).
     - Permitir consultas eficientes e escaláveis.

**O Caminho `/mnt/` no Contexto do Databricks**

No Databricks, o diretório `/mnt/` é um **ponto de montagem (mount point)** que conecta o cluster do Databricks a um sistema de armazenamento externo, como:

- **AWS S3**
- **Azure Data Lake Storage (ADLS)**
- **Google Cloud Storage (GCS)**

Esse ponto de montagem permite que o Databricks acesse e gerencie os dados como se estivessem no sistema de arquivos local, enquanto na realidade estão em um serviço de armazenamento remoto.

---

**Como `/mnt/` Funciona**

1. **Configuração Inicial:**
   - O diretório `/mnt/` é configurado pelo administrador do Databricks ou pelo usuário com permissões apropriadas.
   - Ele utiliza comandos de montagem (`dbutils.fs.mount`) para conectar a um bucket S3, uma conta ADLS ou outro armazenamento.

2. **Mapeamento para o Armazenamento Externo:**
   - Cada subdiretório no `/mnt/` representa um caminho no sistema de armazenamento remoto.
   - Por exemplo:
     - `/mnt/delta/bitcoin_price` → `s3://my-databricks-bucket/mnt/delta/bitcoin_price`

3. **Uso no Delta Lake:**
   - Quando você salva dados no caminho `/mnt/delta/bitcoin_price`:
     - Os arquivos são fisicamente gravados no bucket correspondente no S3, mas podem ser acessados no Databricks como se fossem locais.
   - O Delta Lake organiza os arquivos em:
     - **Arquivos de Dados:** Salvos no formato **Parquet**.
     - **Delta Logs (`_delta_log`):** Gerenciam transações ACID e versionamento.

---

**O Caminho `/mnt/delta/bitcoin_price`**
No contexto do seu pipeline:

1. **Localização Física:**
   - Este caminho está mapeado para o sistema de armazenamento remoto (ex.: AWS S3).
   - Exemplo: `s3://my-databricks-bucket/delta/bitcoin_price/`

2. **Estrutura dos Dados:**
   - Diretório com:
     - **Arquivos Parquet:** Contêm os dados salvos.
     - **Diretório `_delta_log`:** Armazena logs transacionais, permitindo histórico e consistência.

3. **Acessibilidade:**
   - No Databricks, os dados podem ser acessados diretamente pelo caminho `/mnt/delta/bitcoin_price` em APIs PySpark:
     ```python
     df = spark.read.format("delta").load("/mnt/delta/bitcoin_price")
     df.show()
     ```

**Resumo**
O diretório `/mnt/` é o ponto de integração entre o Databricks e sistemas de armazenamento externo, como AWS S3. No seu caso:
- **`/mnt/delta/bitcoin_price`** é um subdiretório montado para armazenar os dados do pipeline no Delta Lake.
- Ele organiza os dados em formato Delta, garantindo eficiência, consistência e suporte a transações ACID.
- É uma prática recomendada para pipelines escaláveis e integrados com sistemas de armazenamento na nuvem.

---

3. **Rotina de Execução:**
   - Esse código é projetado para ser executado a cada 15 minutos, de forma automatizada, via **Databricks Workflow**.

---

### **Como Colocar o Código em um Workflow no Databricks**

#### **1. Subir o Código no Databricks**
- Salve o código como um **notebook** no Databricks. Exemplo: `bitcoin_price_ingestion`.

#### **2. Criar o Workflow**
1. Acesse a aba **Workflows** no Databricks.
2. Clique em **Create Job**.
3. Configure o Workflow:
   - **Name:** Nome do job, por exemplo: `Bitcoin Price Ingestion`.
   - **Task:** Nome da task, por exemplo:  `bitcoin_price_ingestion`
   - **Notebook:** Selecione o notebook `src/bitcoin_price_ingestion` que contém o código.
   - **Cluster:** Escolha ou configure um cluster para executar o job.

A diferença entre um **cluster Serverless** e um **cluster normal** no Databricks está na forma como os recursos são gerenciados, provisionados e otimizados. Aqui está uma explicação detalhada no `README-cluster.md`

   - **Schedule:** Defina para executar a cada 15 minutos:
     - **Cron Expression:** `0 */15 * ? * * *` (executa a cada 15 minutos).

4. **Adicionar Regras de Falha:**
   - Configure o Workflow para:
     - Reexecutar em caso de falha (ex.: 3 tentativas, com intervalo de 5 minutos).

---

### **Rotina para Inserir Dados a Cada 15 Minutos**

Sim, é necessário garantir que cada execução insira os dados de forma incremental na **Delta Table**. Isso é feito automaticamente ao usar o modo `append` no código de escrita:

```python
data_df.write.format("delta").mode("append").save("/mnt/delta/bitcoin_price")
```

#### **Como Funciona a Rotina Incremental?**
1. **Cada Execução (15 Minutos):**
   - Um novo registro é consumido da API.
   - O dado é salvo como uma nova linha na tabela Delta.

2. **Delta Lake Logs (`_delta_log`):**
   - Cada inserção é registrada no log transacional, garantindo que não haja conflitos ou inconsistências.

---

### **Como Jogar os Dados em uma Tabela Gerenciada pelo Databricks**

Agora que os dados estão armazenados no **Delta Lake** no caminho `/mnt/delta/bitcoin_price/`, você pode registrar esse local como uma **tabela gerenciada pelo Databricks** usando SQL ou PySpark.

---

#### **1. Criar uma Tabela Gerenciada no Databricks**
Para registrar a tabela no catálogo, use o seguinte comando SQL no Databricks:

```sql
CREATE TABLE bitcoin_price
USING DELTA
LOCATION '/mnt/delta/bitcoin_price';
```

#### **2. Explicação do Comando**
- **`CREATE TABLE`**: Cria a tabela no catálogo do Databricks.
- **`USING DELTA`**: Especifica que os dados no caminho são armazenados no formato Delta.
- **`LOCATION`**: Aponta para o caminho dos dados no Delta Lake.

#### **3. Resultado**
- Os metadados da tabela são registrados no catálogo (Unity Catalog ou Hive Metastore, dependendo da configuração).
- Agora você pode consultar a tabela diretamente no Databricks:
  ```sql
  SELECT * FROM bitcoin_price;
  ```

---

### **Resumo de Todo o Processo**

Aqui está o resumo detalhado de todo o processo, desde o consumo da API até o armazenamento no Databricks e na AWS:

---

#### **1. Escrita da API**
- Você consumiu os dados da **API Coinbase** para obter o preço do Bitcoin.
- O dado foi estruturado no formato JSON e convertido para um **DataFrame Spark** com schema definido.

---

#### **2. Armazenamento no Databricks**
- O comando de escrita no Delta Lake:
  ```python
  data_df.write.format("delta").mode("append").save("/mnt/delta/bitcoin_price")
  ```
  salvou os dados no caminho **`/mnt/delta/bitcoin_price/`**.

- **O que acontece aqui?**
  - Os dados foram gravados como arquivos **`Parquet`**.
  - Um diretório `_delta_log` foi criado para gerenciar as transações (log de mudanças no Delta Lake).

---

#### **3. Armazenamento na AWS**
- O caminho **`/mnt/delta/bitcoin_price/`** está montado em um **bucket S3**, como parte da integração do Databricks com o armazenamento AWS.
- Os dados ficam fisicamente armazenados no bucket S3 configurado para o ambiente Databricks, por exemplo:
  ```
  s3://my-databricks-bucket/delta/bitcoin_price/
  ```
- **O que fica no S3?**
  - Arquivos de dados (`Parquet`) contendo o conteúdo salvo.
  - Diretório `_delta_log`, que registra as operações no Delta Lake para versionamento e transações ACID.

---

#### **4. Registro no Catálogo**
- **Unity Catalog ou Hive Metastore:**
  - O catálogo armazena os **metadados da tabela**.
  - Quando você usa `CREATE TABLE ... LOCATION`, ele registra o schema da tabela e o caminho no Delta Lake.
- **Benefício:**
  - Permite consultar os dados sem precisar especificar o caminho físico.
  - Exemplo de consulta:
    ```sql
    SELECT * FROM bitcoin_price;
    ```

---

#### **5. Consulta e Uso**
- Após o registro, você pode:
  - Consultar os dados no Databricks com SQL ou PySpark.
  - Utilizar a tabela em jobs e workflows para análises automatizadas.
  - Monitorar o acesso e as permissões via Unity Catalog (se configurado).

---

### **Fluxo Completo**
1. **API Coinbase:** Consome os dados.
2. **Delta Lake no Databricks:**
   - Dados armazenados no caminho `/mnt/delta/bitcoin_price/`.
   - Fisicamente localizado no S3.
3. **Catálogo:** Registra os metadados da tabela com `CREATE TABLE`.
4. **Consultas:** Permite acessar dados diretamente pelo nome da tabela no Databricks.

---

Se precisar de ajuda para configurar ou entender algum passo específico, é só avisar! 😊