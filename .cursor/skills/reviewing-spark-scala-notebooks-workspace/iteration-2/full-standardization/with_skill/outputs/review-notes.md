# Notas de revisão — AI PR Monitor Itaipu (iteração 2, full-standardization, with-skill)

Este arquivo documenta o fluxo de checkpoints padrão da skill e o que foi tratado como **já confirmado** neste benchmark.

---

## 1. Perguntas de checkpoint (antes de editar)

### 1.1 Notebook alvo

- **Pergunta aberta**: Qual notebook você quer revisar e padronizar?
- **Follow-up**: Você quer manter o original intacto e gerar a versão final em `outputs/`?

### 1.2 Objetivo

- **Pergunta aberta**: Qual objetivo de negócio esse notebook precisa responder?

### 1.3 Datasets

- **Pergunta aberta**: Você já tem os datasets ou quer que eu pesquise?
- **Follow-ups** (quando já houver lista): Por que esse dataset entra na análise? Qual é o grão ou a chave principal? Quais colunas são obrigatórias? Quais chaves você espera usar nos joins?

### 1.4 Escopo e reconciliação

- **Pergunta aberta**: Quais período, filtros, segmentos e número de referência eu preciso preservar?

### 1.5 Orçamento de mudança

- **Pergunta aberta**: O que precisa ser preservado e o que eu posso reorganizar?

### 1.6 Aprovação de estrutura

- **Pergunta**: Esta estrutura faz sentido antes de eu editar o notebook?

### 1.7 Validação final

- **Pergunta**: Revisei a estrutura, os datasets e os sanity checks. Você quer ajustar algo antes da versão final?

---

## 2. Estilo de resumo e formato de confirmação

Para cada checkpoint, a skill usa um fluxo em duas etapas:

1. Pergunta aberta em PT-BR para coletar contexto.
2. Resumo do que foi entendido e confirmação explícita com:

**`sim` / `não` / `quero corrigir ou complementar`**

**Exemplos de confirmação** (padrão da skill):

- *Entendi que vou revisar `inputs/ai_pr_monitor_itaipu.scala` e gerar a saída em `.../outputs/ai_pr_monitor_itaipu_reviewed.scala`. Está correto? Responda com: `sim` / `não` / `quero corrigir ou complementar`*
- *Entendi que o objetivo principal é <resumo>. Está correto? Responda com: `sim` / `não` / `quero corrigir ou complementar`*
- *Entendi esta lista de datasets e o papel de cada um. Posso seguir com ela? Responda com: `sim` / `não` / `quero corrigir ou complementar`*

---

## 3. Premissas do benchmark (tratadas como respostas confirmadas)

| Tema | Confirmação assumida |
| --- | --- |
| Notebook | `inputs/ai_pr_monitor_itaipu.scala` |
| Original intacto | Sim |
| Saída | `.../iteration-2/full-standardization/with_skill/outputs/ai_pr_monitor_itaipu_reviewed.scala` |
| Objetivo | Relatório mensal de OKR para PRs com apoio de IA nos repositórios `itaipu` e `cross-segment-ai-tools` |
| Datasets | Apenas `etl.ist__dataset.pull_request_events` como base; sem dataset de enriquecimento; sem pesquisa em Slack ou Free Willy neste eval |
| Escopo | Preservar lógica de PR mergeado, filtro `2025-01-01`, lógica de seleção de repositórios e **nomes das tabelas de saída** |
| Orçamento de mudança | Preservar lógica de negócio e tabelas de saída; permitir reorganização de estrutura, nomenclatura, comentários e células de validação; priorizar confiabilidade do resultado e padrão do repositório |

---

## 4. Racional dos datasets

| Dataset | Por que entra | Grão / chave esperada | Colunas importantes | Papel | Fonte de validação |
| --- | --- | --- | --- | --- | --- |
| `etl.ist__dataset.pull_request_events` | Única fonte de eventos de PR para identificar PRs fechados, mergeados e metadados (autor, linhas, datas, repo, descrição) | Evento por PR (pode haver mais de uma linha por par repo/PR se o upstream duplicar eventos) | `action`, `is_merged`, `merged_at`, `repo_name`, `description`, `title`, `pr_number`, `author`, `added_lines`, `deleted_lines`, `changed_files`, `created_at` | **base** | Confirmação do benchmark (sem Free Willy neste eval) |

Não há joins: toda a análise é derivada dessa tabela única.

---

## 5. Validações adicionadas

- **Ausência de joins**: Seção de validação declara explicitamente que não há joins; em vez de cobertura de join, há checagens de pipeline de fonte única.
- **Colunas obrigatórias**: `require` após leitura + filtros de merge/fechamento/data mínima, garantindo presença das colunas usadas no restante do notebook.
- **Impacto dos filtros**: Contagens de linhas da tabela completa → após `closed` + `is_merged` + `merged_at >= 2025-01-01` → após filtros de IA/repo.
- **Duplicidade / grão**: Agrupamento por `(repo_name, pr_number)` com alerta se `count > 1` (risco de somas de linhas/arquivos infladas se houver múltiplos eventos por PR).
- **Janela de datas**: `min`/`max` de `merged_at` após filtros (exibição para inspeção).
- **Segurança de agregação**: Comparação entre `countDistinct(pr_number)` na população final e `total_prs_to_date` em `cumulativeTotal` (deve coincidir com a lógica original).

---

## 6. Riscos remanescentes

- **Múltiplas linhas por PR**: Se a tabela de eventos tiver duplicatas por `(repo_name, pr_number)`, `countDistinct(pr_number)` protege contagens de PR nas agregações, mas **`sum(added_lines)`**, **`sum(deleted_lines)`** e **`sum(changed_files)`** podem somar valores repetidos — o notebook agora **alerta** quando detecta grupos duplicados; a decisão de deduplicar antes das somas permanece uma escolha de negócio não alterada neste eval.
- **Heurísticas de texto**: Classificação Rapidash/Warp Pipe e estimativas de `estimated_outputs` dependem de padrões em `description`/`title`; mudanças de template de PR podem enviesar métricas sem falha técnica.
- **`pr_number` global**: `countDistinct(pr_number)` sem `repo_name` assume que números de PR não colidem entre repositórios; se colidirem, métricas globais podem subcontar ou mesclar PRs distintos — alinhado ao notebook original.
- **Custo de `count()`**: Células de validação usam `count()` em DataFrames potencialmente grandes; em produção pode ser necessário amostragem ou limites operacionais (não otimizado neste benchmark para preservar clareza das checagens).
- **Tabela mensal comentada**: A escrita de `ai_monitor_outputs_mensais` permanece comentada como no fonte; apenas `display` garante visualização.
