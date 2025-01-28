### **Request for Proposal (RFP): Automação e Integração de Dados em uma Plataforma de Ativos Digitais**

---

#### **1. Contextualização**

A empresa, uma plataforma de negociação de ativos digitais, gerencia grandes volumes de dados relacionados a **transações de compra e venda de Bitcoin** realizadas pelos clientes. Além disso, a operação depende da integração com uma **API de cotação de Bitcoin** para obter o preço atual do ativo, essencial para os cálculos de saldos das carteiras e relatórios financeiros.

Com o crescimento da base de clientes e o aumento do volume de transações, é necessário desenvolver um processo automatizado e eficiente para a ingestão e integração de dados provenientes de diferentes fontes, como **bancos de dados transacionais**, **APIs externas** e **arquivos JSON armazenados em nuvem**.

---

#### **2. Objetivo do Projeto**

O objetivo principal é criar um **pipeline de dados automatizado** que permita:
1. **Ingestão incremental e contínua de dados transacionais de um banco de dados PostgreSQL.**
2. **Atualização em tempo real com base em arquivos JSON carregados em um bucket S3.**
3. **Integração periódica com uma API para sincronizar a cotação do Bitcoin.**
4. **Processamento e estruturação dos dados em tabelas otimizadas para análises e relatórios.**

---

#### **3. Escopo do Projeto**

O escopo contempla a criação de pipelines para automação da ingestão, processamento e transformação dos dados, utilizando ferramentas modernas de engenharia de dados.

---

#### **3.1. Ingestão de Dados Transacionais**
- Configurar um pipeline que extraia, transforme e carregue os dados de transações armazenados em um banco de dados PostgreSQL.
- Frequência: **Uma vez por dia**.
- Estrutura de tabela destino:
  - **Tabela: `transactions`**
    - `transaction_id` (String): Identificador único.
    - `customer_id` (String): ID do cliente.
    - `transaction_type` (String): Tipo de transação (compra ou venda).
    - `amount_btc` (Decimal): Quantidade de BTC transacionada.
    - `transaction_date` (Timestamp): Data e hora da transação.

---

#### **3.2. Integração com Arquivos JSON**
- Desenvolver um pipeline que monitore continuamente um bucket S3 para ingestão de novos arquivos JSON.
- Frequência: **Tempo real** (uso do **Autoloader** do Databricks para ingestão incremental).
- Estrutura de tabela destino:
  - **Tabela: `customer_wallets`**
    - `customer_id` (String): Identificador único do cliente.
    - `name` (String): Nome do cliente.
    - `email` (String): E-mail do cliente.
    - `btc_balance` (Decimal): Saldo em Bitcoin.
    - `usd_balance` (Decimal): Saldo em USD.
    - `last_update` (Timestamp): Última atualização do saldo.

---

#### **3.3. Integração com API de Cotação**
- Criar um pipeline que consulte periodicamente uma API de cotação de Bitcoin para obter o preço mais recente.
- Frequência: **A cada 15 minutos**.
- Estrutura de tabela destino:
  - **Tabela: `bitcoin_price`**
    - `amount` (Decimal): Preço do Bitcoin em USD.
    - `base` (String): Moeda base (ex.: BTC).
    - `currency` (String): Moeda de destino (ex.: USD).
    - `datetime` (Timestamp): Data e hora da cotação.

---

#### **3.4. Processamento e Validação dos Dados**
- Consolidar os dados das tabelas `transactions`, `customer_wallets` e `bitcoin_price` para garantir a consistência e a integridade das informações.
- Realizar validações automáticas, como:
  - Comparar saldos calculados com transações registradas.
  - Verificar a presença de campos obrigatórios nos dados de entrada.
  - Auditar discrepâncias nos dados de cotações e transações.

---

#### **3.5. Organização das Tabelas**
Implementar o conceito de **camadas de dados**:
1. **Bronze:** Dados brutos ingeridos de cada fonte (PostgreSQL, S3, API).
2. **Silver:** Dados limpos e enriquecidos, com validações aplicadas.
3. **Gold:** Tabelas analíticas otimizadas para relatórios e dashboards.

---

#### **4. Entregáveis**
1. Pipelines configurados para ingestão e transformação de dados das três fontes mencionadas.
2. Tabelas organizadas em camadas (Bronze, Silver, Gold) para diferentes finalidades.
3. Documentação técnica descrevendo os pipelines criados, estruturas de tabelas e validações aplicadas.
4. Relatórios básicos sobre:
   - Volume de transações processadas.
   - Média e variação de preço do Bitcoin.
   - Saldos agregados dos clientes.

---

#### **5. Cronograma**
O projeto será concluído em **6 semanas**, com as seguintes etapas principais:
1. **Semana 1-2:** Configuração de ambiente, análise de requisitos e ingestão de dados do PostgreSQL.
2. **Semana 3:** Configuração do Autoloader para ingestão de arquivos JSON.
3. **Semana 4:** Integração com a API de cotação de Bitcoin.
4. **Semana 5:** Consolidação e validação dos dados.
5. **Semana 6:** Entrega final e ajustes.