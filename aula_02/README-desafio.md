### **Request for Proposal (RFP): Sistema de Automação para Atualização de Carteiras de Clientes, Transações e Integração com API de Cotação de Bitcoin**

---

#### **1. Contextualização**

A empresa, uma plataforma de negociação de ativos digitais, gerencia grandes volumes de dados relacionados a **transações de compra e venda de Bitcoin** realizadas pelos clientes. Além disso, a operação depende da integração com uma **API de cotação de Bitcoin** para obter o preço atual do ativo, que influencia diretamente nas operações e nos cálculos de saldos das carteiras dos clientes.

Atualmente, o processo de atualização dos saldos dos clientes e a ingestão da cotação do Bitcoin é realizado **manualmente**, o que causa:

1. **Problemas de Confiabilidade:**
   - Erros manuais no cálculo dos saldos.
   - Dependência de processos manuais para atualizar cotações e dados.

2. **Ineficiência Operacional:**
   - Alto custo em termos de tempo e esforço para consolidar e validar dados.
   - Atrasos na integração dos preços atualizados com as operações.

3. **Impacto na Experiência do Cliente:**
   - Informações desatualizadas ou incorretas sobre os saldos e preços reduzem a confiança na plataforma.

Com o crescimento do volume de transações e da base de clientes, a empresa precisa de um sistema automatizado para integrar dados de transações, atualizar as carteiras e sincronizar cotações da API em tempo real.

---

#### **2. Objetivo do Projeto**

O objetivo principal é implementar um **sistema de automação escalável e confiável** para:

1. **Sincronizar dados de transações (`transactions`) e carteiras de clientes (`customer_wallets`).**
2. **Integrar automaticamente a cotação do Bitcoin de uma API externa.**
3. **Garantir a consistência dos saldos e fornecer visibilidade operacional.**

---

#### **3. Escopo do Projeto**

O projeto deve incluir as seguintes funcionalidades:

---

#### **3.1. Automação da Atualização de Saldos**
- Desenvolver uma solução que atualize automaticamente os campos `btc_balance` e `usd_balance` na tabela **`customer_wallets`**, com base nas transações registradas na tabela **`transactions`**.
- Suportar **compra** e **venda** de Bitcoin, ajustando os saldos em tempo real ou em intervalos definidos.
- Garantir que a solução funcione para grandes volumes de transações, mantendo a precisão.

---

#### **3.2. Integração com a API de Cotação**
- Conectar-se à **API de cotação do Bitcoin** para obter o preço atualizado (exemplo: API da Coinbase).
- Salvar os dados da API em uma tabela **`bitcoin_price`**, com as colunas:
  - `amount`: Preço do Bitcoin em USD.
  - `base`: Moeda base (ex.: BTC).
  - `currency`: Moeda de destino (ex.: USD).
  - `datetime`: Data e hora da cotação.
- Garantir que o sistema utilize a cotação mais recente para calcular o valor em USD nas transações de Bitcoin.

---

#### **3.3. Processamento de Transações Históricas**
- Processar retroativamente todas as transações registradas desde **01/01/2025** até o momento atual.
- Garantir a consistência dos saldos das carteiras dos clientes com base nas transações históricas e nas cotações armazenadas na tabela **`bitcoin_price`**.

---

#### **3.4. Interface para Monitoramento**
- Criar uma interface para os administradores:
  - Monitorar atualizações automáticas de saldos e ingestão de cotações.
  - Visualizar discrepâncias ou erros nos cálculos.
  - Reexecutar a sincronização ou ajustar saldos manualmente, se necessário.

---

#### **3.5. Geração de Relatórios e KPIs**
- Implementar relatórios para analisar:
  - **Volume de Transações:** Total de compras e vendas realizadas em um período.
  - **Saldos dos Clientes:** Total de BTC e USD por cliente.
  - **Preço Médio de Compra/Venda:** Análise detalhada de preços médios das transações.
  - **Variações de Preço:** Impacto das cotações no volume de negociações.

---

#### **4. Requisitos Técnicos**
- **Escalabilidade:** O sistema deve suportar crescimento no número de transações e na base de clientes.
- **Alta Disponibilidade:** Garantir que o sistema funcione com alta confiabilidade e mínimos períodos de inatividade.
- **Integração:** 
  - Banco de dados SQL (PostgreSQL, MySQL ou similar).
  - Suporte a tabelas Delta no Databricks.
  - API de cotação de Bitcoin.
- **Segurança:** Controlar permissões para acesso e edição de dados, evitando alterações não autorizadas.

---

#### **5. Entregáveis**
1. Sistema automatizado para:
   - Atualizar saldos dos clientes (`customer_wallets`).
   - Integrar e registrar cotações de Bitcoin (`bitcoin_price`).
2. Scripts para processamento retroativo de dados históricos.
3. Interface para monitoramento e auditoria.
4. Relatórios e dashboards para KPIs.
5. Documentação técnica e manual do usuário.

---

#### **6. Cronograma e Prazo**
O projeto deverá ser concluído no prazo de **60 dias** após a assinatura do contrato, com as seguintes etapas principais:
1. **Semana 1-2:** Levantamento de requisitos e design da solução.
2. **Semana 3-5:** Desenvolvimento e integração.
3. **Semana 6-7:** Testes e validação.
4. **Semana 8:** Treinamento e entrega final.

---

#### **7. Proposta de Parceria**
Buscamos uma empresa/parceiro com:
- Experiência em sistemas de automação e integração de dados.
- Conhecimento em tecnologias como Python, Databricks, APIs REST e bancos de dados SQL.
- Capacidade de oferecer suporte e manutenção após a entrega.