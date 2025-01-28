### **Cenário: Pegar a API a Cada 15 Minutos**
- **Execução Frequente:** A tarefa será acionada 96 vezes por dia (24h ÷ 15 min = 96 execuções).
- **Duração Curta:** Geralmente, consumir uma API e salvar dados no Delta Lake é uma tarefa rápida, levando de alguns segundos a poucos minutos.
- **Carga Leve:** O volume de dados consumido em cada execução é pequeno (1 chamada API por vez).

---

### **Recomendação**
**Usar um Cluster Serverless** é a melhor opção neste caso. Eis o porquê:

#### **Por Que Cluster Serverless?**
1. **Custo-Efetivo:**
   - Como o job será executado apenas a cada 15 minutos e dura pouco tempo, você só paga pelos recursos usados durante o processamento.
   - Com clusters normais, o cluster ficaria ocioso durante os intervalos de 15 minutos, mas continuaria gerando custos.

2. **Escalabilidade Automática:**
   - O Serverless ajusta automaticamente os recursos para processar rapidamente a tarefa e libera-os em seguida.

3. **Simplicidade:**
   - Não é necessário configurar tipos de instância ou escalabilidade manual. O Databricks gerencia tudo automaticamente.

4. **Execuções de Curta Duração:**
   - Jobs que duram poucos minutos são ideais para o Serverless, já que ele minimiza o tempo e o custo de provisionamento.

---

### **Quando Usar Cluster Normal?**
Você pode considerar um cluster normal nos seguintes casos:
- **Workload Complexo ou Longo:**
  - Se a ingestão da API incluir etapas mais demoradas, como transformações complexas ou chamadas adicionais a outras APIs.
- **Requer Controle Total:**
  - Se precisar de configurações específicas no cluster, como instâncias com alto desempenho ou GPUs.
- **Reutilização Constante:**
  - Se o cluster será usado para outros jobs no intervalo de 15 minutos, diminuindo o tempo gasto para reiniciar o cluster.

---

### **Configuração no Databricks Workflow**
#### **1. Com Cluster Serverless**
- Configure o Workflow:
  1. No campo **Compute**, selecione **Serverless**.
  2. Defina a frequência de execução como **a cada 15 minutos** (cron expression: `0 */15 * ? * * *`).
  3. Anexe o notebook com o código que consome a API e salva no Delta Lake.

#### **2. Com Cluster Normal**
- Configure o Workflow:
  1. No campo **Compute**, selecione um cluster normal já configurado.
  2. Ative **Auto-Termination** no cluster para que ele seja desligado após o job, reduzindo custos.
  3. Defina a frequência como **a cada 15 minutos**.

---

### **Resumo**
- **Escolha Principal:** Use **Cluster Serverless** para tarefas leves e frequentes como consumir uma API e salvar dados no Delta Lake.
- **Justificativa:** Reduz custos, elimina sobrecarga de gerenciamento e é otimizado para execuções curtas.
- **Cluster Normal:** Recomendado apenas se houver processos adicionais rodando no mesmo cluster ou necessidade de controle específico.