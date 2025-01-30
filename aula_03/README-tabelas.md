# **📌 Tipos de Tabelas e Views no Databricks (Delta Lake)**
Ao projetar a arquitetura **Bronze → Silver → Gold**, precisamos decidir se usamos **tabelas físicas** ou **views**. Cada abordagem tem **vantagens e desvantagens**, e a escolha depende do **uso do dado** e da **performance desejada**.

---

## **1️⃣ Tabelas vs. Views**
No Databricks (e no Delta Lake), podemos armazenar e consultar dados de diferentes maneiras:
- **Tabelas físicas (Delta Tables)** → Dados persistidos no storage.
- **Views (Dinâmicas e Materializadas)** → Dados calculados no momento da consulta.

Cada abordagem tem casos de uso específicos.

---

## **2️⃣ Tipos de Tabelas no Databricks**
O Databricks suporta diferentes tipos de **tabelas Delta**.

| Tipo | Persistência | Performance | Atualização | Melhor Para |
|------|-------------|-------------|-------------|-------------|
| **Managed Table** (Tabela Gerenciada) | Sim | Alta | Atualização automática | Dados críticos e gerenciados pelo Databricks |
| **External Table** (Tabela Externa) | Sim | Média | Manual (via Jobs) | Dados que precisam ficar no storage externo |
| **Materialized View** | Sim | Alta | Atualização programada | Dashboards e análises de alto desempenho |
| **View** (View Normal) | Não | Média | Sempre atualizada | Consultas rápidas sem persistência |

---

## **3️⃣ Comparação: Tabela Delta vs. View**
Agora, vamos entender **quando usar cada tipo**.

### **📌 Tabela Delta (Managed ou External)**
✅ **Vantagens**  
- **Performance superior** → Dados são armazenados fisicamente.  
- **Suporte a transações ACID** → Segurança e integridade.  
- **Pode ser usada para análises históricas**.  
- **Permite otimizações como `OPTIMIZE`, `Z-ORDER` e `VACUUM`**.

❌ **Desvantagens**  
- **Ocupa mais armazenamento**.  
- **Requer um processo ETL para atualização**.  

### **📌 View (Dinâmica)**
✅ **Vantagens**  
- **Sempre reflete os dados mais recentes**.  
- **Ocupa pouco ou nenhum armazenamento**.  
- **Boa para análises exploratórias**.  

❌ **Desvantagens**  
- **Reprocessa os dados toda vez que é consultada** → Pode ser lenta.  
- **Menos otimizada para consultas complexas**.  

---

## **4️⃣ O que usar na Arquitetura Medalhão?**
Agora aplicamos esse conhecimento na nossa arquitetura **Bronze → Silver → Gold**.

| Camada | Melhor Tipo | Justificativa |
|--------|------------|--------------|
| **Bronze** | **Tabela Delta (External Table)** | Permite ingestão contínua e evita perda de dados. |
| **Silver** | **Tabela Delta (Managed Table)** | Mantém os dados limpos e tipados, otimizando performance. |
| **Gold** | **Tabela Delta (Managed Table) ou Materialized View** | Performance otimizada para análises e KPIs. |
| **Dashboards** | **Materialized View ou View** | Melhor para consultas rápidas e dashboards. |

---

## **5️⃣ Quando Usar Materialized Views?**
Uma **Materialized View** é um meio-termo entre **tabela física** e **view dinâmica**:
- **Boa para dashboards que precisam ser rápidos**.
- **Atualiza automaticamente conforme um job programado**.
- **Pode ser usada para KPIs e métricas históricas**.

**Exemplo de Materialized View para KPIs:**
```sql
CREATE MATERIALIZED VIEW gold.kpi_summary AS
SELECT 
    COUNT(transaction_id) AS total_transactions,
    SUM(transaction_value_in_usd) AS total_volume_usd,
    AVG(transaction_value_in_usd) AS avg_transaction_value
FROM gold.transactions;
```

Isso **melhora a performance** para dashboards, sem recalcular os KPIs em tempo real.

---

# **🏁 Conclusão**
### **O que usar em cada camada?**
✅ **Bronze → Tabelas Delta Externas** (Melhor para ingestão)  
✅ **Silver → Tabelas Delta Gerenciadas** (Dados limpos e padronizados)  
✅ **Gold → Tabelas Delta ou Materialized Views** (Performance para KPIs)  
✅ **Dashboards → Views ou Materialized Views** (Melhor tempo de resposta)  

🚀 **Essa abordagem equilibra performance, armazenamento e atualização eficiente dos dados!**