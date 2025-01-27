# Primeira aula

Nosso objetivo nessa primeira aula é configurar nosso ambiente no Databricks, ter uma visão da arquitetura geral do Spark (e do Databricks) e vamos ter um hands-on de código.

## Configuração Databricks

## Overview arquitetura Spark 

1. **Básico**  
   Introdução ao Apache Spark, seus conceitos fundamentais e aplicação no gerenciamento de grandes volumes de dados.  

2. **Principais Componentes como Driver e Worker**  
   Explicação sobre a arquitetura do Spark, incluindo os papéis do Driver (controle) e dos Workers (execução).  

Em um cluster Spark, o **executor** é uma entidade crucial responsável por realizar as tarefas (tasks) atribuídas a ele, processando dados e retornando resultados para o **driver**. Vamos analisar os conceitos relacionados aos executors e como eles funcionam no Databricks:

### **Executors no Spark**
1. **Tarefas e Execução**:
   - Os executors executam as **tasks**, que são as unidades de trabalho paralelas no Spark.
   - Cada executor tem um conjunto de **slots**, que determinam quantas tasks ele pode executar simultaneamente (geralmente definido pelo número de cores atribuídos a ele).

2. **Memória e Dados**:
   - Cada executor tem sua própria memória para armazenar dados em cache e executar operações.
   - A memória de um executor é compartilhada entre as tasks e armazenamentos intermediários (como o RDD ou dataframe em cache).

---

### **Executors no Databricks**
No Databricks, a configuração padrão é **1 executor por nó de trabalho (worker node)**. Isso ocorre porque:
1. **Gerenciamento Simplificado**:
   - O Databricks otimiza os recursos alocando um executor por nó de trabalho, reduzindo a complexidade do gerenciamento de memória e comunicação entre executors.

2. **Performance**:
   - O uso de 1 executor por nó garante que todos os recursos do nó (CPU, memória) sejam dedicados a esse executor, maximizando a performance sem sobrecarga de competição entre múltiplos executors.

3. **Escalabilidade**:
   - Cada nó pode ser ajustado verticalmente (com mais recursos) ou o cluster pode ser ajustado horizontalmente (com mais nós).

4. **Cluster Gerenciado**:
   - No Databricks, o cluster é gerenciado de forma a manter simplicidade e eficiência, o que inclui essa decisão de design.

---

### **Comparação: Spark Geral vs. Databricks**
| Característica             | Spark Geral                | Databricks                |
|----------------------------|----------------------------|---------------------------|
| Executors por Worker Node  | Pode ser múltiplos         | Sempre 1:1               |
| Gerenciamento de Recursos  | Configuração manual        | Automatizado             |
| Memória Compartilhada      | Entre múltiplos executors  | Inteiramente para 1 executor |

Essa arquitetura é parte da estratégia do Databricks para facilitar o uso e otimizar a performance de clusters gerenciados.

#### Slots

### Ponto Importante: Slots e Núcleos de CPU

Cada **executor** no Spark possui um número de **slots** para executar as tasks simultaneamente, que está diretamente relacionado ao número de **núcleos de CPU** alocados para aquele executor.

#### Como Funciona:
1. **Slots e Núcleos**:
   - Um slot equivale a um núcleo lógico de CPU disponível para executar uma task.
   - Por exemplo, se um executor tem 4 núcleos de CPU alocados, ele terá 4 slots para executar até 4 tasks simultaneamente.

2. **Recursos no Worker Node**:
   - A quantidade de slots disponíveis no executor deve ser dimensionada de acordo com os recursos totais do nó de trabalho (worker node), garantindo que os núcleos sejam suficientes para suportar as tasks sem causar sobrecarga ou competição excessiva.

3. **Databricks e Configuração de Slots**:
   - No **Databricks**, como há um executor por nó de trabalho, todos os núcleos disponíveis no nó são atribuídos a esse único executor, garantindo que o número de slots seja igual à quantidade de núcleos lógicos disponíveis.

---

### **Importância dos Slots para Performance**
- **Número Suficiente de Slots**:
  - Um número insuficiente de slots pode causar gargalos, com tasks esperando para serem processadas.
  - Um número excessivo de slots (mais do que os núcleos disponíveis) pode levar a **overhead**, já que o sistema tenta dividir os recursos além do limite ideal.

- **Ajuste Ideal**:
  - O número de slots deve ser igual ou menor ao número de núcleos de CPU disponíveis no nó de trabalho.
  - No Databricks, isso já é ajustado automaticamente, otimizando o uso dos recursos.

---

Essa configuração alinhada entre slots e núcleos garante eficiência no processamento e maximiza o throughput em clusters Spark, especialmente em ambientes gerenciados como o Databricks.

### Pontos Relevantes sobre Executors e Execução no Spark

1. **Tasks e Slots**:
   - Um executor pode executar tantas **tasks** em paralelo quanto o número de **slots** disponíveis, que por sua vez é determinado pelo número de núcleos de CPU alocados ao executor.
   - **Task**: É a menor unidade de trabalho no Spark. Cada task processa um único **partition** de dados.

   **Exemplo**:
   - Se o cluster tiver **2 worker nodes**, cada um com **4 núcleos de CPU**, o total será de **8 slots**.
   - Isso significa que até **8 tasks** podem ser executadas simultaneamente no cluster.

2. **Partições e Tarefas**:
   - Os dados são divididos em **partitions**, que formam a base para as tasks no Spark.
   - Cada partition é processada por uma única task. Portanto, o número de partitions define o número mínimo de tasks necessárias para processar os dados.

3. **Ambiente de Execução (JVM)**:
   - Tanto o **driver** quanto os **executors** são processos que rodam na **JVM** (Java Virtual Machine).
   - O **driver** coordena a execução geral do aplicativo, enquanto os executors realizam o trabalho de processar as partitions e retornar os resultados.

---

### Exemplo Prático
- **Cenário**:
  - Cluster com 2 workers, cada um com 4 núcleos de CPU (8 slots no total).
  - Um dataset é dividido em 16 partitions.

- **Execução**:
  - Inicialmente, 8 tasks serão atribuídas para execução, uma para cada slot.
  - Assim que uma task é concluída, o slot fica disponível para processar outra partition até que todas as 16 tasks sejam completadas.

---

### Resumo
- **Tasks em Paralelo**: Limitadas pelo número de slots no cluster.
- **Partições**: Devem ser configuradas adequadamente para equilibrar o trabalho entre os executors.
- **Execução na JVM**: Facilita a integração e interoperabilidade do Spark com várias linguagens (como Python, Scala, e Java). 

Esse design escalável e eficiente faz do Spark uma ferramenta poderosa para processar grandes volumes de dados.

3. **Spark UI**  
   Apresentação da interface Spark UI para monitorar e depurar jobs em execução.  

### Métricas spark UI

Esses dados são métricas de **performance** exibidas no **Spark UI**, que ajudam a entender como o trabalho do Spark foi executado. Vamos detalhar cada ponto para que você compreenda o que significam e como podem ser analisados:

---

### 1. **Task Deserialization Time**
- **O que é?**: O tempo gasto para **deserializar** (interpretar) o código da tarefa no executor.
- **Por que importa?**: Isso acontece no início de cada tarefa. Se esse tempo for alto, pode indicar problemas com o tamanho ou complexidade dos objetos que estão sendo enviados para os executores.

---

### 2. **Duration**
- **O que é?**: A duração total da tarefa, do início ao fim.
- **Por que importa?**: É a métrica mais óbvia para saber quanto tempo uma tarefa levou. Altos tempos podem indicar gargalos no processamento ou leitura de dados.

---

### 3. **GC Time (Garbage Collection Time)**
- **O que é?**: O tempo gasto pelo **Garbage Collector (GC)** limpando memória inutilizada.
- **Por que importa?**: Se o tempo de GC for alto, pode significar que sua aplicação está consumindo muita memória ou que os objetos não estão sendo liberados eficientemente.

---

### 4. **Result Serialization Time**
- **O que é?**: O tempo gasto para **serializar** (empacotar) o resultado da tarefa antes de enviá-lo de volta ao driver.
- **Por que importa?**: Geralmente é pequeno, mas valores altos podem indicar que os resultados da tarefa são grandes ou complexos, dificultando a transmissão.

---

### 5. **Getting Result Time**
- **O que é?**: O tempo gasto para o driver receber os resultados da tarefa.
- **Por que importa?**: Geralmente é insignificante. Se for alto, pode indicar lentidão na comunicação entre o executor e o driver.

---

### 6. **Scheduler Delay**
- **O que é?**: O tempo que a tarefa ficou esperando antes de ser executada.
- **Por que importa?**: Um alto **Scheduler Delay** pode significar:
  - Não há recursos suficientes no cluster (como CPU ou memória).
  - O cluster está muito ocupado.
  - Problemas com a configuração do paralelismo.

---

### 7. **Peak Execution Memory**
- **O que é?**: A maior quantidade de memória usada pela tarefa durante a execução.
- **Por que importa?**: Se for muito alta, pode haver falta de memória no executor, levando a falhas ou ao uso de disco (swap).

---

### 8. **Input Size / Records**
- **O que é?**: O tamanho e o número de registros processados pela tarefa.
  - **Input Size**: O volume de dados que a tarefa leu (em MiB).
  - **Records**: Quantos registros foram lidos.
- **Por que importa?**: Permite entender a distribuição do trabalho. Desequilíbrios podem significar **skew** (dados mal distribuídos).

---

### 9. **Shuffle Write Size / Records**
- **O que é?**: O tamanho e o número de registros escritos durante o **shuffle**.
  - **Shuffle Write Size**: O volume de dados escrito para o shuffle.
  - **Records**: Quantos registros foram enviados no shuffle.
- **Por que importa?**: Altos valores indicam que a tarefa está enviando muitos dados para outros executores, o que pode ser um gargalo.

---

### 10. **Shuffle Write Time**
- **O que é?**: O tempo gasto para escrever dados no shuffle.
- **Por que importa?**: Um tempo alto pode indicar problemas de disco, rede, ou simplesmente que muitas tarefas estão tentando escrever ao mesmo tempo.

---

## Como usar essas métricas?

### Identificando problemas comuns:
1. **Tasks demorando muito (Duration)**:
   - Verifique **GC Time**, **Scheduler Delay**, e **Input Size**.
   - Talvez precise aumentar os recursos ou otimizar a lógica.

2. **Problemas de memória (GC Time ou Peak Execution Memory)**:
   - Reduza o uso de objetos grandes ou ajuste configurações como `spark.executor.memory`.

3. **Dados mal distribuídos (Skew)**:
   - Verifique **Input Size / Records** e **Shuffle Write Size**. Se algumas tarefas têm dados muito maiores que outras, você pode precisar redistribuí-los.

Essas métricas te ajudam a entender o comportamento do Spark e otimizar seus jobs para melhor desempenho! 🚀

4. **Modos de Deploy (Deployment Modes)**  
   Diferentes formas de implementar o Spark: local, cluster e client-server.  

### Deployment Models no Apache Spark

O Apache Spark suporta vários **modelos de deployment**, que determinam como o **driver**, os **executors** e os **worker nodes** interagem durante a execução de um job. Os principais modelos são: **local**, **client**, e **cluster**.

---

### 1. **Local Deployment**
- **Descrição**:
  - O Spark roda tudo (driver e executors) no **mesmo processo** ou na mesma máquina.
  - Ideal para desenvolvimento, teste ou processamento de pequenos volumes de dados.

- **Características**:
  - Não exige configuração de cluster.
  - Simula a execução distribuída em uma máquina local.
  - Geralmente usado no modo de desenvolvimento com o comando `local[N]`, onde **N** é o número de threads (ex.: `local[4]` usa 4 threads).

- **Vantagens**:
  - Simplicidade: Não há necessidade de gerenciar recursos distribuídos.
  - Ideal para testes rápidos e debugging.

- **Limitações**:
  - Não escalável para grandes volumes de dados.
  - Limitado pelos recursos da máquina local.

---

### 2. **Client Deployment**
- **Descrição**:
  - O **driver** roda na máquina do cliente (a partir do qual o job é enviado), enquanto os **executors** rodam nos **worker nodes** do cluster.
  - O cliente mantém a comunicação direta com os executors.

- **Características**:
  - O driver precisa estar ativo durante toda a execução do job.
  - Usado geralmente para interatividade, como no Spark Shell ou notebooks.

- **Vantagens**:
  - Simples de configurar em um cluster existente.
  - Permite controle direto do job a partir do cliente.

- **Limitações**:
  - Dependente da máquina do cliente: se ela falhar, o job será interrompido.
  - Latência maior se o cliente estiver distante do cluster (por exemplo, em diferentes regiões).

---

### 3. **Cluster Deployment**
- **Descrição**:
  - Tanto o **driver** quanto os **executors** rodam no cluster. O job é enviado ao cluster e gerenciado completamente por ele.
  - Modelos comuns de clusters incluem **YARN**, **Kubernetes**, e **Standalone**.

- **Características**:
  - O cluster gerencia toda a execução do job, independentemente do cliente.
  - O cliente envia o job, mas não precisa permanecer ativo.

- **Vantagens**:
  - Escalabilidade: Projetado para grandes volumes de dados.
  - Resiliência: Menos dependência do cliente, maior tolerância a falhas.
  - Flexibilidade: Suporte a vários gerenciadores de cluster.

- **Limitações**:
  - Configuração mais complexa em comparação com o modelo local ou client.
  - Pode exigir recursos adicionais para gerenciar o cluster.

---

### Comparação dos Modelos

| Característica       | **Local**          | **Client**           | **Cluster**        |
|----------------------|--------------------|----------------------|--------------------|
| **Driver**           | Local              | Na máquina do cliente| No cluster         |
| **Executors**        | Local              | No cluster           | No cluster         |
| **Escalabilidade**   | Baixa              | Média                | Alta               |
| **Uso Comum**        | Testes/Desenvolvimento | Jobs interativos     | Processamento em larga escala |
| **Resiliência**      | N/A                | Baixa (dependente do cliente) | Alta               |

---

Esses modelos oferecem flexibilidade ao Spark para atender a diferentes casos de uso, desde desenvolvimento local até processamento de grandes volumes de dados em clusters distribuídos. A escolha do modelo depende da escala, interatividade e requisitos do projeto.


5. **RDDs, DataFrames e Datasets**  
   Comparação entre as principais abstrações de dados no Spark e suas aplicações práticas.  

### Comparação entre RDD, DataFrame e Dataset no Apache Spark

O Apache Spark fornece três APIs principais para processar dados: **RDD**, **DataFrame** e **Dataset**. Cada uma delas tem suas características, vantagens e desvantagens, dependendo do caso de uso.

---

### **1. Resilient Distributed Dataset (RDD)**
- **Descrição**:
  - É a API mais básica e de baixo nível do Spark.
  - Representa uma coleção imutável de objetos distribuídos por um cluster.

- **Características**:
  - **Imutável**: Não pode ser alterado após a criação.
  - **Distribuído**: Dividido em várias partições que são processadas paralelamente.
  - **Sem Esquema**: Trabalha diretamente com objetos ou coleções de dados, sem informações de tipo ou esquema.
  - **Lazy Evaluation**: Operações não são executadas imediatamente, mas apenas quando uma ação (como `collect()`) é chamada.

- **Vantagens**:
  - Flexibilidade para manipular qualquer tipo de dado.
  - Controle granular sobre o processamento de dados.

- **Desvantagens**:
  - Menos otimizado: Não aproveita as otimizações do Catalyst e Tungsten.
  - Requer mais código para tarefas comuns (como filtragem e agrupamento).

---

### **2. DataFrame**
- **Descrição**:
  - API de alto nível baseada em **esquema** que representa dados em um formato tabular (semelhante a uma tabela SQL ou um dataframe do pandas).
  - Implementado sobre RDD, mas com muitas otimizações internas.

- **Características**:
  - **Esquema**: Colunas nomeadas e tipos de dados definidos.
  - **Otimizações**: Usa o Catalyst Optimizer para otimizar consultas.
  - **Abstração**: Fornece uma interface declarativa semelhante ao SQL.

- **Vantagens**:
  - Fácil de usar com operações SQL-like.
  - Melhor desempenho devido a otimizações internas.
  - Suporte a várias fontes de dados (CSV, Parquet, JSON, etc.).

- **Desvantagens**:
  - Não é tão flexível quanto RDD para manipulação de tipos de dados personalizados.
  - Fracamente tipado (em comparação com Dataset).

---

### **3. Dataset**
- **Descrição**:
  - API que combina os benefícios do RDD e DataFrame.
  - Oferece um **esquema** como o DataFrame, mas é **fortemente tipado**, permitindo maior controle e segurança em tempo de compilação.

- **Características**:
  - **Fortemente Tipado**: Usa classes case no Scala e tipos no Java.
  - **Esquema**: Define colunas e tipos como o DataFrame.
  - **Operações de Alto Nível**: Semelhante ao SQL e DataFrame.
  - **Compatibilidade**: Pode ser convertido facilmente entre RDD e DataFrame.

- **Vantagens**:
  - Mais seguro e fácil de depurar devido ao suporte a tipos.
  - Otimizações automáticas via Catalyst Optimizer.
  - Combina a expressividade do RDD com a eficiência do DataFrame.

- **Desvantagens**:
  - Pode ser mais verboso do que DataFrame.
  - Disponível apenas em Scala e Java (não em Python).

---

### **Comparação Direta**

| Aspecto               | **RDD**                       | **DataFrame**                 | **Dataset**                   |
|-----------------------|-------------------------------|-------------------------------|-------------------------------|
| **Tipo**              | Sem esquema                  | Esquema fraco                | Esquema forte (tipado)        |
| **Otimizações**       | Sem otimizações              | Catalyst Optimizer            | Catalyst Optimizer            |
| **Performance**       | Mais lento                   | Mais rápido                   | Mais rápido                   |
| **Verificação de Tipo** | Em tempo de execução         | Em tempo de execução          | Em tempo de compilação        |
| **Facilidade de Uso** | Complexo                     | Fácil com SQL-like API        | Médio                         |
| **Flexibilidade**     | Alta                         | Média                         | Alta                          |
| **Suporte a Linguagens** | Scala, Java, Python          | Scala, Java, Python, R        | Scala, Java                   |

---

### **Quando Usar Cada API**

1. **RDD**:
   - Quando você precisa de controle total sobre o processamento de dados.
   - Para manipular tipos de dados personalizados.
   - Quando você está migrando aplicativos antigos baseados em RDD.

2. **DataFrame**:
   - Para processamento de dados estruturados/tabulares.
   - Quando o desempenho é crítico e você quer aproveitar as otimizações automáticas.
   - Para análises SQL-like ou integração com ferramentas de BI.

3. **Dataset**:
   - Quando você precisa de verificações de tipo em tempo de compilação.
   - Para pipelines de ETL que requerem transformações complexas com segurança de tipo.
   - Quando você quer combinar a facilidade do DataFrame com a flexibilidade do RDD.

---

### Conclusão

Escolha **RDD** para flexibilidade bruta, **DataFrame** para simplicidade e performance, e **Dataset** para segurança de tipo e expressividade. Cada API tem seus casos de uso, e a escolha depende do seu cenário específico.


6. **Transformações e Ações (Transformations and Actions)**  
   Explicação sobre operações no Spark, distinguindo entre aquelas que preparam o pipeline (transformações) e as que executam os cálculos (ações).  

Em sistemas de processamento de dados como o Spark, **Transformações** e **Ações** são os dois tipos principais de operações realizadas em conjuntos de dados. Apesar de ambos serem operações, eles têm finalidades e comportamentos distintos:

---

### **Transformações**
- **O que são?** Operações que criam um novo conjunto de dados a partir de um existente, sem executar imediatamente o processamento.
- **Características principais:**
  - São **lazy** (preguiçosas), ou seja, não executam imediatamente. Apenas descrevem como os dados devem ser transformados.
  - Retornam um novo **RDD** (ou DataFrame/Dataset) representando a transformação aplicada.
  - Exemplos: `map()`, `filter()`, `groupByKey()`, `reduceByKey()`, `select()`, etc.
  - São **encadeáveis**: você pode aplicar várias transformações em sequência antes de executar o processamento.

#### **Exemplo:**
```python
# Aplicando uma transformação (lazy)
dados_filtrados = dados.filter(lambda x: x['idade'] > 18)
```
Nesse momento, nenhum dado foi processado. Apenas foi criada a definição do que deve ser feito.

---

### **Ações**
- **O que são?** Operações que iniciam o processamento e retornam um resultado ou salvam os dados em algum local.
- **Características principais:**
  - São **eager** (executadas imediatamente). Ao serem chamadas, disparam o pipeline de transformações descrito anteriormente.
  - Retornam um **valor** ou **escrevem dados** em um destino (como arquivos ou bancos de dados).
  - Exemplos: `count()`, `collect()`, `take(n)`, `saveAsTextFile()`, `show()`, etc.

#### **Exemplo:**
```python
# Executando uma ação (eager)
resultado = dados_filtrados.collect()
print(resultado)
```
Aqui, o processamento é realizado, aplicando o filtro definido anteriormente, e os dados resultantes são coletados.

---

### **Resumo das diferenças:**

| Aspecto               | Transformações                          | Ações                              |
|-----------------------|------------------------------------------|------------------------------------|
| **Execução**          | Lazy (não executa imediatamente)        | Eager (executa imediatamente)     |
| **Objetivo**          | Criar um novo conjunto de dados         | Retornar resultados ou salvar dados |
| **Retorno**           | Novo RDD/DataFrame                     | Valor ou escrita no armazenamento |
| **Exemplos**          | `map`, `filter`, `select`, `groupByKey` | `count`, `collect`, `show`, `saveAsTextFile` |

---

Ambos trabalham em conjunto: **as transformações definem o que será feito, e as ações disparam o processamento necessário para gerar os resultados.**


7. **Jobs, Stages e Tasks**  
   Estrutura de execução do Spark: como um job é dividido em estágios e tarefas para otimizar o processamento.  


## API e hands-on Spark