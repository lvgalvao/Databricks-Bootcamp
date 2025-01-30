# **📜 Regras de Negócio e Implementação Completa da Arquitetura Medalhão (Silver → Gold)**

Este documento define **as regras de negócio e a implementação completa** das tabelas **Silver** e **Gold**, garantindo um pipeline de dados eficiente, correto e pronto para análise.

---

# **📌 Introdução: Objetivo do Projeto e Importância para os Analistas**

## **📍 Contexto**
O mercado de criptomoedas está em constante evolução, e a volatilidade dos preços do Bitcoin impacta diretamente as transações realizadas pelos clientes. Para garantir uma **análise precisa e confiável**, é fundamental estruturar os dados corretamente, permitindo que os analistas tomem **decisões estratégicas** baseadas em informações sólidas.

## **🎯 Objetivo do Projeto**
O objetivo deste projeto é construir uma **arquitetura de dados eficiente** para monitorar transações de clientes envolvendo Bitcoin, garantindo a rastreabilidade do saldo original e atualizado em **USD e BTC**. Através da **camada Gold**, os analistas poderão:
✅ **Acompanhar o saldo atualizado dos clientes** considerando todas as compras e vendas de BTC.  
✅ **Analisar o impacto da volatilidade do Bitcoin** nas transações.  
✅ **Entender o comportamento dos clientes**, identificando padrões de compra e venda.  
✅ **Otimizar relatórios financeiros**, permitindo insights estratégicos sobre os gastos e investimentos dos clientes.  

## **🔍 Justificativa**
Sem uma estrutura de dados bem organizada, os analistas teriam dificuldades em:
❌ Rastrear corretamente os saldos originais e as movimentações de BTC e USD.  
❌ Relacionar as transações com o preço correto do Bitcoin no momento da compra/venda.  
❌ Criar relatórios eficientes sem necessidade de cálculos manuais complexos.  

Com essa solução, a equipe terá **dados confiáveis e acessíveis**, prontos para serem explorados via dashboards e análises preditivas, garantindo um **melhor entendimento do mercado e das transações dos clientes**. 🚀

---

# **1️⃣ Regras de Negócio para a Camada Silver**
A **Silver** é a camada onde os dados são tratados e padronizados. Aqui aplicamos **três regras principais**:

### **📌 Regra 1: Remover Clientes Inválidos**
- **Descrição:** Clientes inválidos não devem ser incluídos na camada Silver nem na Gold.
- **Critério:** Remover **'Mark Cunningham'**, **'Mark Savage'**
- **Aplicação:** `silver.customers` já deve conter apenas clientes válidos.

### **📌 Regra 2: Considerar Apenas Transações Válidas**
- **Descrição:** Apenas transações entre **29/01/2025 e 30/01/2025** são consideradas válidas.
- **Critério:** Qualquer transação fora deste intervalo deve ser descartada.
- **Aplicação:** `silver.transactions` deve filtrar os dados antes de chegarem na Gold.

### **📌 Regra 3: Manter `btc_balance_original` e `usd_balance_original`**
- **Descrição:** Devemos armazenar o saldo original de BTC e USD, pois o saldo atualizado será calculado na Gold.
- **Critério:** **Os saldos originais vêm da `bronze.customers`** e **não devem ser alterados na Silver**.

---

## **🔹 1️⃣ Implementação das Tabelas Silver**
Aqui estão as **queries para a Silver**, garantindo que os dados sejam tratados corretamente.

### **🟢 Criando `silver.customers`**
```sql
CREATE OR REPLACE TABLE silver.customers AS 
SELECT 
    customer_id,
    name,
    email,
    CAST(usd_balance AS DOUBLE) AS usd_balance_original,  -- Mantendo saldo original de USD
    CAST(btc_balance AS DOUBLE) AS btc_balance_original,  -- Mantendo saldo original de BTC
    CAST(last_update AS TIMESTAMP) AS last_update
FROM bronze.customers
WHERE name NOT IN ('Mark Cunningham', 'Mark Savage');  -- Removendo clientes inválidos
```

✅ **Agora `usd_balance_original` e `btc_balance_original` representam o saldo original dos clientes**.  
✅ **Mantemos `last_update` para rastrear quando o saldo foi atualizado**.  

---

### **🟢 Criando `silver.transactions`**
```sql
CREATE OR REPLACE TABLE silver.transactions AS 
SELECT 
    transaction_id,
    customer_id,
    CAST(transaction_type AS STRING) AS transaction_type,  -- Forçando para STRING
    btc_amount,
    transaction_date
FROM bronze.transactions
WHERE DATE(transaction_date) BETWEEN '2025-01-29' AND '2025-01-30'; -- Regra de transações válidas
```

✅ **Somente transações dos dias 29 e 30 de janeiro são consideradas**.  
✅ **Mantemos `transaction_type` para calcular corretamente os saldos na Gold**.  

---

### **🟢 Criando `silver.bitcoin_price`**
```sql
CREATE OR REPLACE TABLE silver.bitcoin_price AS 
SELECT 
    datetime AS price_timestamp,
    currency,
    amount AS btc_price
FROM bronze.bitcoin_price;
```

✅ **Essa tabela mantém o preço do Bitcoin para ser usado na camada Gold**.  

---

# **2️⃣ Regras de Negócio para a Camada Gold**
A **Gold** é a camada onde os dados são enriquecidos e agregados. Aqui aplicamos **três regras principais**:

### **📌 Regra 1: Calcular `transaction_value_in_usd`**
- **Descrição:** Cada transação deve ser convertida para USD, pegando a **cotação mais próxima do BTC** no momento da transação.
- **Critério:** Utilizar `LAG()` para obter o **timestamp anterior mais próximo** da cotação.

### **📌 Regra 2: Criar `btc_balance_final` e `usd_balance_final`**
- **Descrição:** Calculamos o **saldo final de BTC e USD**, levando em consideração todas as transações do cliente.
- **Critério:**
  - `btc_balance_final` = `btc_balance_original` **+ compras - vendas**.
  - `usd_balance_final` = `usd_balance_original` **+ total gasto em USD**.

### **📌 Regra 3: Ajustar `btc_amount` para Compra (Soma) e Venda (Subtrai)**
- **Descrição:** Para calcular corretamente o saldo final de BTC, devemos **somar o `btc_amount` nas compras e subtrair nas vendas**.

---

## **🔹 2️⃣ Implementação das Tabelas Gold**
Aqui estão as **queries para a Gold**, garantindo que os dados sejam enriquecidos corretamente.

### **🟡 Criando `gold.transactions`**
```sql
CREATE OR REPLACE TABLE gold.transactions AS 
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

✅ **Agora `transaction_value_in_usd` reflete corretamente o valor em dólares**.  
✅ **Se for compra, o valor fica negativo. Se for venda, fica positivo**.  

---

### **🟡 Criando `gold.customers`**
```sql
CREATE OR REPLACE TABLE gold.customers AS 
SELECT 
    c.customer_id,
    c.name,
    c.email,
    c.usd_balance_original,  -- Mantendo saldo original de USD
    c.btc_balance_original,  -- Mantendo saldo original de BTC
    -- Calculando saldo atualizado de BTC considerando compras e vendas
    c.btc_balance_original + COALESCE(SUM(
        CASE 
            WHEN t.transaction_type = 'compra' THEN t.btc_amount  -- Se comprou, adiciona BTC
            WHEN t.transaction_type = 'venda' THEN -t.btc_amount  -- Se vendeu, reduz BTC
            ELSE 0
        END
    ), 0) AS btc_balance_final,
    COUNT(t.transaction_id) AS total_transactions,
    -- Calculando total de USD gasto somando transaction_value_in_usd com usd_balance_original
    c.usd_balance_original + COALESCE(SUM(t.transaction_value_in_usd), 0) AS usd_balance_final
FROM silver.customers c
LEFT JOIN gold.transactions t ON c.customer_id = t.customer_id
GROUP BY c.customer_id, c.name, c.email, c.usd_balance_original, c.btc_balance_original;
```

✅ **Agora `btc_balance_final` reflete todas as compras e vendas realizadas corretamente**.  
✅ **Agora `usd_balance_final` inclui corretamente `usd_balance_original` somado ao total gasto em transações**.  

---

# **🏁 Conclusão**
Agora, a **arquitetura está 100% ajustada e pronta para análise!** 🎯  

✅ **Silver está limpa, filtrada e padronizada**.  
✅ **Gold tem transações e saldos calculados corretamente**.  
✅ **Os analistas podem consumir a Gold diretamente para relatórios e dashboards**.  

🚀 **Agora seus dados estão estruturados para análise financeira eficiente!** 🔥