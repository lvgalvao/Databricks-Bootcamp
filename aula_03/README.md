Para visualizar o **schema das tabelas diretamente no Databricks**, você tem algumas opções:

---

### **1. Usando o Data Explorer (Interface do Databricks)**
Se estiver na interface web do Databricks:
1. **Vá para a aba "Data"** no menu lateral esquerdo.
2. **Navegue até o catálogo `bronze` e clique na tabela desejada** (`customers`, `transactions` ou `bitcoin_price`).
3. A estrutura da tabela será exibida, incluindo os tipos de dados e detalhes do schema.

---

### **2. Usando o Comando SQL**
Dentro de um **notebook do Databricks**, execute:

```sql
DESCRIBE bronze.customers;
DESCRIBE bronze.transactions;
DESCRIBE bronze.bitcoin_price;
```

Ou para uma visão mais detalhada:

```sql
DESCRIBE DETAIL bronze.customers;
DESCRIBE DETAIL bronze.transactions;
DESCRIBE DETAIL bronze.bitcoin_price;
```

Isso mostrará informações sobre os **tipos de dados das colunas**, **localização da tabela no storage**, **número de arquivos**, **formato da tabela (Delta, Parquet, etc.)**, entre outros.

---

### **3. Usando PySpark no Notebook**
Se estiver usando **Python (PySpark)** no notebook do Databricks:

```python
# Importando a sessão do Spark
spark.table("bronze.customers").printSchema()
spark.table("bronze.transactions").printSchema()
spark.table("bronze.bitcoin_price").printSchema()
```

Isso imprimirá a estrutura hierárquica da tabela diretamente no notebook.

---

### **4. Explorando os Dados Visualmente**
Se quiser ver os dados junto com os tipos das colunas:
1. Rode um **SELECT no notebook**:

   ```sql
   SELECT * FROM bronze.customers LIMIT 10;
   ```
   
2. No **Databricks UI**, ao visualizar o resultado, clique em **"Columns"** para ver os tipos de dados.

---

### **Conclusão**
Se quiser **ver no UI**, use a aba **"Data"** no Databricks.  
Se quiser **ver no notebook**, use `DESCRIBE`, `printSchema()` ou um `SELECT`. 🚀