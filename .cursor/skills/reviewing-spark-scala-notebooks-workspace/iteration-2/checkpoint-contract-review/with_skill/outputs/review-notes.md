# Notas de revisão — benchmark checkpoint-contract-review (iteração 2, with_skill)

Este arquivo documenta o **fluxo de checkpoints padrão** que seria executado *antes* de editar o notebook, conforme `reviewing-spark-scala-notebooks` / `examples.md`.

---

## Formato de cada checkpoint

1. **Pergunta aberta (PT-BR)** — coletar contexto.
2. **Resumo do que foi entendido** — em uma ou duas frases, objetivo e limites.
3. **Confirmação obrigatória** — sempre encerrar com:

`Está correto? Responda com: sim / não / quero corrigir ou complementar`

---

## Checkpoints exatos (perguntas antes de editar)

### 1. Notebook alvo

**Pergunta:**  
`Qual notebook você quer revisar e padronizar? Você quer manter o original intacto e gerar a versão final em outputs/?`

**Estilo de resumo após a resposta:**  
`Entendi que vou revisar <caminho do arquivo> e entregar a versão final em <caminho de saída>, sem alterar o original.`

**Confirmação:**  
`Está correto? Responda com: sim / não / quero corrigir ou complementar`

---

### 2. Objetivo de negócio

**Pergunta:**  
`Qual objetivo de negócio esse notebook precisa responder?`

**Estilo de resumo após a resposta:**  
`Entendi que o objetivo principal é <resumo do objetivo em uma frase>.`

**Confirmação:**  
`Está correto? Responda com: sim / não / quero corrigir ou complementar`

---

### 3. Datasets

**Pergunta:**  
`Você já tem os datasets ou quer que eu pesquise? Para cada dataset: por que ele entra na análise, qual o grão/chave, colunas obrigatórias, chaves de join e papel (base, enriquecimento, filtro, benchmark, output).`

**Estilo de resumo após a resposta:**  
`Entendi esta lista de datasets, o grão esperado e o papel de cada um na análise.`

**Confirmação:**  
`Posso seguir com ela? Responda com: sim / não / quero corrigir ou complementar`

---

### 4. Escopo e reconciliação

**Pergunta:**  
`Quais período, filtros, segmentos e número de referência (dashboard/tabela oficial) eu preciso preservar? Há alguma restrição que não pode ser quebrada?`

**Estilo de resumo após a resposta:**  
`Entendi que o escopo é <período, filtros principais, benchmark se houver>.`

**Confirmação:**  
`Está correto? Responda com: sim / não / quero corrigir ou complementar`

---

### 5. Orçamento de mudanças

**Pergunta:**  
`O que precisa ser preservado e o que eu posso renomear ou reorganizar? A prioridade é legibilidade, performance, padronização ou confiabilidade do resultado?`

**Estilo de resumo após a resposta:**  
`Entendi que devo <preservar X / permitir Y / priorizar Z>.`

**Confirmação:**  
`Está correto? Responda com: sim / não / quero corrigir ou complementar`

---

### 6. Aprovação de estrutura

**Pergunta:**  
`Antes de editar: esta estrutura em inglês faz sentido — Intro/README, Imports, Data sources, Widgets, Variable filters, Functions, Code/analysis, Validation, Display — com validações para colunas obrigatórias, duplicidade no grão de PR e segurança de agregações?`

**Estilo de resumo após a resposta:**  
`Entendi que a estrutura aprovada inclui as seções canônicas e o plano de sanity checks descritos.`

**Confirmação:**  
`Está correto? Responda com: sim / não / quero corrigir ou complementar`

---

### 7. Validação final (antes da entrega)

**Pergunta:**  
`Revisei estrutura, datasets e sanity checks. Quer ajustar algo antes da versão final?`

**Estilo de resumo após a resposta:**  
`Entendi que <não há ajustes / há os seguintes ajustes: ...>.`

**Confirmação:**  
`Responda com: sim / não / quero corrigir ou complementar`

---

## Por que a pesquisa de datasets (Free Willy / Slack) **não** é acionada neste caso

- O usuário **já definiu** o conjunto de dados: apenas `etl.ist__dataset.pull_request_events`.
- Não há **lacunas, ambiguidade ou risco óbvio** que exijam descoberta de novas fontes (fluxo híbrido do skill: pesquisar só quando o usuário pede pesquisa ou a lista tem gaps/risco).
- O benchmark fixou explicitamente **“no dataset research is needed”**, o que está alinhado ao ramo “validar datasets existentes contra o objetivo” em vez do ramo “dataset research branch”.

---

## Respostas tratadas como confirmadas neste benchmark (pós-checkpoint)

| Tema | Confirmação efetiva |
| --- | --- |
| Objetivo | Relatório mensal de OKR para PRs com apoio de IA |
| Datasets | Somente `etl.ist__dataset.pull_request_events` |
| Pesquisa | Não necessária |
| Preservação | Lógica de negócio e **nomes das tabelas de saída** |
| Estrutura | Reorganizar para o padrão do repositório (seções canônicas em inglês) |
