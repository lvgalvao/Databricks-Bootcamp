# **🚀 Atualizando Materialized Views e Tabelas no Databricks (Silver → Gold)**  
Manter as **tabelas Delta e Materialized Views atualizadas** é essencial para garantir **dados confiáveis e frescos** para análise. Neste post, detalhamos o **processo completo de atualização** das tabelas **Silver e Gold** no Databricks, refletindo as regras de negócio e otimizando a performance do pipeline.

---

# **📌 1️⃣ Como Atualizar as Tabelas Silver e Gold no Databricks**
As tabelas **Silver** e **Gold** precisam ser **atualizadas periodicamente** para garantir que os dados estejam consistentes. Podemos usar **três estratégias principais**.

### **🔹 Estratégia 1: `INSERT OVERWRITE` (Reprocessamento Total)**
Se a tabela precisa ser **recalculada periodicamente**, podemos sobrescrevê-la:

```sql
INSERT OVERWRITE TABLE gold.transactions
WITH btc_price AS (
    SELECT 
        price_timestamp, 
        btc_price,
        LAG(price_timestamp) OVER (ORDER BY price_timestamp) AS prev_timestamp
    FROM silver.bitcoin_price
)
SELECT 
    t.transaction_id,
    t.customer_id,
    t.transaction_type,
    t.btc_amount,
    t.transaction_date,
    ROUND(
        CASE 
            WHEN t.transaction_type = 'compra' THEN (t.btc_amount * p.btc_price) * -1  
            ELSE (t.btc_amount * p.btc_price)  
        END, 2
    ) AS transaction_value_in_usd
FROM silver.transactions t
JOIN btc_price p
    ON t.transaction_date BETWEEN p.prev_timestamp AND p.price_timestamp;
```

✅ **Ótimo para cenários onde o histórico precisa ser reprocessado periodicamente.**  
❌ **Pode ser custoso se houver grandes volumes de dados.**

---

### **🔹 Estratégia 2: `MERGE INTO` (Atualização Incremental)**
Se queremos apenas **atualizar novos registros sem reprocessar tudo**, usamos `MERGE`:

```sql
MERGE INTO gold.transactions AS g
USING (
    SELECT 
        t.transaction_id,
        t.customer_id,
        t.transaction_type,
        t.btc_amount,
        t.transaction_date,
        ROUND(
            CASE 
                WHEN t.transaction_type = 'compra' THEN (t.btc_amount * p.btc_price) * -1  
                ELSE (t.btc_amount * p.btc_price)  
            END, 2
        ) AS transaction_value_in_usd
    FROM silver.transactions t
    JOIN silver.bitcoin_price p
        ON t.transaction_date BETWEEN p.prev_timestamp AND p.price_timestamp
) AS s
ON g.transaction_id = s.transaction_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
```

✅ **Evita reprocessamento completo e melhora a eficiência**.  
✅ **Ótimo para cargas incrementais**.  
❌ **Pode ser mais lento que `INSERT OVERWRITE` caso haja muitos updates**.

---

### **🔹 Estratégia 3: Streaming com `AUTO LOADER` (Atualização Contínua)**
Se os dados chegam continuamente, podemos usar **streaming**:

```python
from pyspark.sql.functions import col

silver_transactions = spark.readStream.format("delta").load("dbfs:/silver/transactions")

silver_transactions.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "dbfs:/checkpoints/transactions") \
    .table("gold.transactions")
```

No **Databricks SQL**, não temos um comando direto para **streaming**, mas podemos configurar um **Delta Live Table (DLT)**, que permite **processamento contínuo de dados** e substitui o Auto Loader para ingestão incremental.

Aqui está a **versão SQL para criar um streaming contínuo**, usando **Delta Live Tables**:

---

### **🔹 Estratégia 3: Streaming com `AUTO LOADER` (SQL Version - Delta Live Tables)**
Se os dados chegam continuamente, podemos criar uma **tabela em modo streaming**.

```sql
CREATE LIVE TABLE gold.transactions
AS
SELECT 
    t.transaction_id,
    t.customer_id,
    t.transaction_type,
    t.btc_amount,
    t.transaction_date,
    ROUND(
        CASE 
            WHEN t.transaction_type = 'compra' THEN (t.btc_amount * p.btc_price) * -1  
            ELSE (t.btc_amount * p.btc_price)  
        END, 2
    ) AS transaction_value_in_usd
FROM STREAM (silver.transactions) t
JOIN STREAM (silver.bitcoin_price) p
    ON t.transaction_date BETWEEN p.price_timestamp AND p.price_timestamp + INTERVAL 1 MINUTE;
```

---

### **📌 Como Isso Funciona?**
1. **`CREATE LIVE TABLE`** → Cria uma tabela **que se atualiza continuamente** conforme novos dados chegam.
2. **`STREAM (silver.transactions)`** → Configura a tabela para **streaming contínuo**, processando novas transações em tempo real.
3. **`STREAM (silver.bitcoin_price)`** → Garante que a cotação do BTC seja usada **em tempo real** para calcular `transaction_value_in_usd`.
4. **`JOIN ... INTERVAL 1 MINUTE`** → Garante que pegamos o preço do Bitcoin mais próximo da transação.

---

### **📌 Como Agendar e Configurar o Streaming no Databricks?**
1. **Acesse Databricks UI → Workflows → Delta Live Tables**.
2. **Crie um novo Pipeline**.
3. **Adicione o SQL acima** como uma **Live Table**.
4. **Configure o modo de execução como "Continuous"** (modo streaming contínuo).
5. **Salve e execute**.

✅ **Agora os dados serão processados continuamente sem necessidade de Jobs manuais!** 🚀

---

✅ **Recomendado para ingestão em tempo real (exemplo: APIs de mercado financeiro).**  
❌ **CARO PARA CACETA**  

---

# **📌 2️⃣ Como Atualizar Materialized Views no Databricks**
As **Materialized Views** armazenam consultas pré-calculadas e devem ser **atualizadas periodicamente**.  
No Databricks, **elas NÃO se atualizam automaticamente**, exigindo um **job programado**.

## **🚀 Atualizando Materialized Views**
1. **Criamos a Materialized View**:

```sql
CREATE MATERIALIZED VIEW gold.transactions AS 
WITH btc_price AS (
    SELECT 
        price_timestamp, 
        btc_price,
        LAG(price_timestamp) OVER (ORDER BY price_timestamp) AS prev_timestamp
    FROM silver.bitcoin_price
)
SELECT 
    t.transaction_id,
    t.customer_id,
    t.btc_amount,
    t.transaction_type,
    -- Valor da transação em USD (compra negativa, venda positiva)
    ROUND(
        CASE 
            WHEN t.transaction_type = 'compra' THEN (t.btc_amount * p.btc_price) * -1  
            ELSE (t.btc_amount * p.btc_price)  
        END, 2
    ) AS transaction_value_in_usd,
    t.transaction_date
FROM silver.transactions t
JOIN btc_price p
    ON t.transaction_date BETWEEN p.prev_timestamp AND p.price_timestamp;
```

2. **Criamos um job no Databricks para atualizar periodicamente**:

```sql
   REFRESH MATERIALIZED VIEW gold.transactions;
```

3. **Agendamos esse comando em um Job no Databricks:**
   - No **Databricks UI**, vá para **Workflows → Jobs**.
   - Clique em **Create Job** e selecione **SQL Query**.
   - Insira o comando:

```sql
REFRESH MATERIALIZED VIEW gold.transactions;
```

   - Configure a **frequência de execução** (exemplo: a cada 30 minutos).

✅ **Ideal para dashboards que precisam de alta performance.**  
❌ **Não reflete mudanças em tempo real** (só quando o job roda).  

---

# **📌 3️⃣ Como Automatizar as Atualizações?**
Podemos usar **Databricks Workflows** para rodar **jobs automáticos**.

### **🚀 Criando um Job para Atualizar as Tabelas Gold e a Materialized View**
1. **Vá para o Databricks UI → Workflows → Jobs**.
2. **Crie um Novo Job**.
3. **Adicione um novo Task** para rodar um **SQL Script**:
   ```sql
   -- Atualiza a tabela gold.transactions
   INSERT OVERWRITE TABLE gold.transactions
   WITH btc_price AS (
       SELECT 
           price_timestamp, 
           btc_price,
           LAG(price_timestamp) OVER (ORDER BY price_timestamp) AS prev_timestamp
       FROM silver.bitcoin_price
   )
   SELECT 
       t.transaction_id,
       t.customer_id,
       t.transaction_type,
       t.btc_amount,
       t.transaction_date,
       ROUND(
            CASE 
                WHEN t.transaction_type = 'compra' THEN (t.btc_amount * p.btc_price) * -1  
                ELSE (t.btc_amount * p.btc_price)  
            END, 2
       ) AS transaction_value_in_usd
   FROM silver.transactions t
   JOIN btc_price p
       ON t.transaction_date BETWEEN p.prev_timestamp AND p.price_timestamp;

   -- Atualiza a tabela gold.customers
   INSERT OVERWRITE TABLE gold.customers
   SELECT 
       c.customer_id,
       c.name,
       c.email,
       c.usd_balance_original,
       c.btc_balance_original,
       c.btc_balance_original + COALESCE(SUM(
            CASE 
                WHEN t.transaction_type = 'compra' THEN t.btc_amount  
                WHEN t.transaction_type = 'venda' THEN -t.btc_amount  
                ELSE 0
            END
       ), 0) AS btc_balance_final,
       COUNT(t.transaction_id) AS total_transactions,
       c.usd_balance_original + COALESCE(SUM(t.transaction_value_in_usd), 0) AS usd_balance_final
   FROM silver.customers c
   LEFT JOIN gold.transactions t ON c.customer_id = t.customer_id
   GROUP BY c.customer_id, c.name, c.email, c.usd_balance_original, c.btc_balance_original;
   ```
4. **Configure a Frequência de Execução** (exemplo: a cada 1 hora).
5. **Execute o Job e monitore no Databricks UI**.

✅ **Agora tudo é atualizado automaticamente sem precisar de intervenção manual!**  

---

### **📌 Comparação Entre Tabela, View e Materialized View no Databricks**  

No **Databricks**, podemos armazenar e acessar dados de diferentes formas:  
- **Tabelas** (persistentes e armazenadas fisicamente).  
- **Views** (consultas dinâmicas, sem armazenamento físico).  
- **Materialized Views** (armazenam resultados pré-calculados para otimizar consultas).  

A escolha entre esses métodos depende do **uso do dado**, **performance desejada** e **necessidade de atualização**.

---

### **📊 Comparação Entre Tabela, View e Materialized View**
| Característica         | **Tabela** (Delta Table) | **View** | **Materialized View** |
|------------------------|-------------------------|----------|-----------------------|
| **Persistência**       | Sim, os dados são armazenados fisicamente. | Não, apenas uma consulta que retorna dados em tempo real. | Sim, os resultados são armazenados fisicamente. |
| **Performance**        | Alta, especialmente com otimizações (`OPTIMIZE`, `Z-ORDER`). | Média, pois precisa recalcular os dados a cada consulta. | Muito alta, pois os dados já estão processados. |
| **Atualização**        | Deve ser atualizada manualmente (`INSERT OVERWRITE`, `MERGE`). | Sempre exibe os dados mais recentes automaticamente. | Deve ser atualizada periodicamente com `REFRESH MATERIALIZED VIEW`. |
| **Uso de Armazenamento** | Ocupa espaço em disco, pois mantém os dados completos. | Nenhum armazenamento adicional, apenas metadados. | Ocupa espaço, pois armazena os resultados da consulta. |
| **Custo Computacional** | Médio, pois os dados já estão estruturados. | Alto, pois os dados precisam ser recalculados a cada execução. | Baixo, pois os resultados já foram pré-calculados. |
| **Ideal Para**         | Dados transacionais, históricos e que precisam ser armazenados permanentemente. | Consultas dinâmicas, ad-hoc e análise em tempo real. | Dashboards, agregações e cálculos que precisam ser acessados rapidamente. |
| **Exemplo de Uso**      | Tabela `gold.transactions` para armazenar transações consolidadas. | View `vw_active_users` para calcular usuários ativos no momento. | Materialized View `gold.kpi_summary` para KPIs financeiros. |

---

### **✅ Quando Usar Cada Uma?**
| **Caso de Uso**                         | **Melhor Escolha**  |
|-----------------------------------------|--------------------|
| Precisa armazenar dados permanentemente | **Tabela** (Delta Table) |
| Consulta sempre precisa refletir os dados mais recentes | **View** |
| KPI ou agregações precisam de alta performance | **Materialized View** |
| Precisa otimizar performance de leitura | **Materialized View ou Tabela** |
| Dados são muito grandes e raramente mudam | **Materialized View** |
| Consulta complexa que precisa ser executada frequentemente | **Materialized View** |

---

### **🏁 Conclusão**
- **Use `TABELA` para armazenar dados que precisam ser persistentes**.  
- **Use `VIEW` para consultas dinâmicas que precisam refletir os dados mais recentes**.  
- **Use `MATERIALIZED VIEW` para otimizar performance em consultas que são feitas repetidamente**.  

🚀 **A escolha certa melhora a eficiência do pipeline e reduz custos computacionais!** 🔥

### **📌 Qual a Diferença Entre Tabela e Materialized View?**  
A **Tabela** e a **Materialized View** parecem similares porque ambas **armazenam dados fisicamente**. No entanto, elas possuem **propósitos diferentes** e oferecem **vantagens distintas** dependendo do cenário.

---

## **📊 Comparação Direta: Tabela vs. Materialized View**

| Característica | **Tabela** (`Delta Table`) | **Materialized View** (`MView`) |
|--------------|-------------------------|-----------------------------|
| **O que é?** | Um armazenamento físico de dados estruturados. | Uma consulta pré-calculada armazenada fisicamente. |
| **Atualização** | Atualizada manualmente (`INSERT OVERWRITE`, `MERGE`). | Deve ser atualizada com `REFRESH MATERIALIZED VIEW`. |
| **Performance** | Alta, especialmente para leituras diretas e bem indexadas. | Muito alta, pois os dados já estão agregados e prontos. |
| **Uso de Armazenamento** | Ocupa espaço para todos os dados da tabela. | Ocupa espaço apenas para os resultados da consulta. |
| **Ideal Para** | Armazenar todos os dados históricos e transacionais. | Agregar e acelerar consultas repetitivas (ex.: KPIs). |
| **Custo Computacional** | Médio, pois pode exigir cálculos ao consultar. | Baixo, pois evita cálculos repetitivos. |
| **Quando Usar?** | Quando os dados precisam ser **modificados** constantemente. | Quando a consulta é **pesada e precisa ser acessada rapidamente**. |

---

## **🔍 Exemplo Prático**
Imagine que temos uma tabela `gold.transactions` com **milhões de transações**.

### **🚀 Caso 1: Tabela Delta (`gold.transactions`)**
```sql
CREATE OR REPLACE TABLE gold.transactions AS 
SELECT 
    transaction_id,
    customer_id,
    transaction_type,
    btc_amount,
    transaction_value_in_usd,
    transaction_date
FROM silver.transactions;
```
✅ **Armazena todas as transações individualmente**.  
❌ **Se for preciso calcular estatísticas, cada consulta pode ser lenta**.  

---

### **🚀 Caso 2: Materialized View para KPIs (`gold.kpi_summary`)**
Se quisermos **analisar rapidamente KPIs financeiros** sem recalcular todas as transações, usamos uma **Materialized View**:

```sql
CREATE MATERIALIZED VIEW gold.kpi_summary AS
SELECT 
    COUNT(transaction_id) AS total_transactions,
    SUM(transaction_value_in_usd) AS total_volume_usd,
    AVG(transaction_value_in_usd) AS avg_transaction_value
FROM gold.transactions;
```

✅ **A consulta já vem pronta e otimizada**.  
✅ **Os cálculos são feitos apenas quando `REFRESH MATERIALIZED VIEW` é rodado**.  
❌ **Os dados não são atualizados automaticamente** (é necessário rodar `REFRESH`).  

---

## **📌 Quando Usar Tabela vs. Materialized View?**

| **Cenário** | **Melhor Escolha** |
|------------|--------------------|
| Precisa armazenar grandes volumes de dados detalhados | **Tabela (Delta Table)** |
| Precisa de alta performance para cálculos agregados | **Materialized View** |
| Dados são consultados frequentemente, mas não mudam com frequência | **Materialized View** |
| Precisa de atualização em tempo real sempre que consultar | **Tabela + View** (não Materialized) |
| Relatórios ou Dashboards acessam os mesmos cálculos repetidamente | **Materialized View** |

---

## **🏁 Conclusão**
- **Use `TABELA` para armazenar todos os dados brutos e detalhados**.  
- **Use `MATERIALIZED VIEW` para agilizar consultas que precisam de cálculos pesados e repetitivos**.  
- **Se a consulta precisa estar sempre atualizada, uma `VIEW` simples pode ser melhor**.  

🚀 **O segredo é combinar os três tipos para otimizar performance e custo!** 🔥