# notebook-ai

Projeto para desenhar e evoluir uma skill capaz de revisar notebooks Scala para Databricks com foco em boas praticas, clareza, reprodutibilidade e um padrao de qualidade predefinido.

O objetivo nao e apenas "melhorar codigo". A meta e sempre chegar a uma versao final do notebook que esteja:

- alinhada a um padrao consistente de estrutura e leitura
- aderente a boas praticas de Spark/Databricks
- validada com sanity checks minimos
- pronta para revisao humana
- salva em `outputs/`

Este repositorio funciona como base de referencia, teste e iteracao para a skill.

## Objetivo

A skill deve ser capaz de atuar em um cenario principal:

1. **Revisar e padronizar um notebook Scala existente**
   - ler um notebook em `inputs/`
   - diagnosticar problemas de estrutura, legibilidade e qualidade
   - propor ajustes passo a passo
   - gerar uma versao final revisada em `outputs/`

## Principio central

O fluxo deve ser **interativo**. A skill nao deve sair escrevendo ou reescrevendo tudo de uma vez.

Ela deve trabalhar passo a passo com o usuario:

1. entender o contexto
2. confirmar objetivo e restricoes
3. propor uma abordagem
4. executar por etapas
5. validar o resultado final contra uma barra de qualidade definida

## Estrutura do repositorio

```text
.
├── .cursor/skills/
│   └── databricks-scala-notebook/
│       ├── SKILL.md
│       ├── reference.md
│       └── examples.md
├── inputs/
│   └── notebooks Spark Scala existentes usados como teste
├── outputs/
│   └── notebooks revisados pela skill
├── templates/
│   └── referencias de boas praticas e aprendizados
└── README.md
```

### Pastas

- `inputs/`: contem notebooks existentes que servem como casos de teste para a skill. Hoje ja existe `Final CC Default Rate.scala`.
- `templates/`: contem o material de referencia que define o padrao esperado. Neste repositorio, os principais insumos sao `boas_praticas.txt`, `01. RAM Analysis by Global Holdout.scala`, `Academia AI.md` e `Repo CLO.scala`.
- `outputs/`: destino das versoes finais produzidas pela skill apos a revisao e padronizacao dos notebooks.

## Referencias utilizadas

As referencias atuais do projeto apontam para um padrao claro:

- notebook com uma historia unica e objetivo explicito no topo
- parametros centralizados no inicio
- imports e helpers separados
- transformacoes organizadas em blocos logicos
- comentarios explicando o motivo, nao apenas o que
- nomes de variaveis e colunas claros
- preferencia por operacoes distribuidas em Spark, evitando `collect()` em dados grandes
- escrita idempotente quando houver materializacao
- sanity checks para volume, periodo, unicidade, nulls, joins, agregacoes e reconciliacao

O notebook de exemplo em `inputs/Final CC Default Rate.scala` tambem reforca um padrao desejado:

- cabecalho forte em markdown com contexto, metricas, fontes e premissas
- secoes explicitas como `Imports`, `External Sources`, `Functions` e `Sanity Checks`
- orientacoes de manutencao e atualizacao
- convencoes de nomes para variaveis e colunas

## Comportamento esperado da skill

### Fluxo de revisao

Quando o usuario pedir para revisar um notebook existente, a skill deve:

1. ler o notebook de origem
2. identificar pontos fortes, problemas e lacunas
3. explicar o que sera ajustado antes de editar
4. reorganizar o notebook mantendo a logica de negocio
5. melhorar estrutura, nomes, comentarios e validacoes
6. preservar ou explicitar premissas importantes
7. gerar uma versao final em `outputs/`

## Interacao minima com o usuario

A skill deve fazer perguntas suficientes para evitar uma resposta generica ou desalinhada. O minimo esperado e confirmar:

- qual notebook sera revisado
- qual e o objetivo de negocio do notebook
- quais tabelas, fontes ou datasets entram no fluxo
- se os datasets ja estao confirmados ou se a skill deve pesquisar e validar
- qual periodo, filtro ou recorte importa
- se existe numero oficial, dashboard ou tabela de referencia para reconciliacao
- quais restricoes nao podem ser quebradas
- qual o nivel de mudanca permitido no notebook original

Se estiver revisando um notebook existente, a skill tambem deve confirmar:

- o que deve ser preservado
- o que pode ser reestruturado
- se a prioridade e legibilidade, performance, padronizacao ou confiabilidade dos resultados

## Barra de qualidade

Para considerar um notebook como "final", ele deve atender a uma barra minima de qualidade.

### 1. Estrutura

- possui cabecalho com contexto, objetivo e premissas
- tem ordem de leitura clara
- separa configuracao, imports, leitura, transformacoes, materializacao e saida
- evita celulas sobrando de exploracoes antigas

### 2. Clareza e manutencao

- nomes de variaveis e colunas sao explicitos
- comentarios registram racional, regra de negocio ou risco
- repeticoes desnecessarias sao reduzidas
- blocos longos demais sao quebrados em funcoes ou helpers quando fizer sentido

### 3. Boas praticas Databricks / Spark

- evita acoes caras sem necessidade
- prefere agregacoes e filtros no cluster
- usa parametros no inicio do notebook
- considera idempotencia ao escrever tabelas ou resultados
- materializa etapas intermediarias quando isso reduz recomputacao ou melhora legibilidade operacional

### 4. Validacao

- inclui sanity checks relevantes para o caso
- verifica contagens, periodo, nulls e duplicidades onde fizer sentido
- observa inflacao ou perda inesperada apos joins
- valida agregacoes e reconciliacao com numeros de referencia quando existirem

### 5. Entrega

- a saida final fica em `outputs/`
- o notebook final pode ser lido por outra pessoa sem depender de contexto oral
- as principais premissas ficam explicitas no proprio notebook

## Definition of Done

Uma entrega so deve ser considerada concluida quando:

- existir uma versao final em `outputs/`
- o notebook estiver organizado e consistente do inicio ao fim
- as premissas de negocio estiverem documentadas
- os sanity checks essenciais estiverem presentes
- o resultado tiver sido construido com checkpoints de alinhamento com o usuario

## Escopo do README

Este README descreve o **objetivo do projeto** e o **comportamento esperado da skill**.

A localizacao final da skill ainda pode ser decidida depois:

- como skill versionada no proprio repositorio
- como skill pessoal do Cursor fora do repositorio

Independentemente da localizacao, este repositorio ja define:

- o problema que a skill precisa resolver
- as referencias que sustentam o padrao de qualidade
- os arquivos de entrada para teste
- a pasta de saida para os resultados finais

## Proximo passo recomendado

O repositorio ja contem a skill em `.cursor/skills/databricks-scala-notebook/`.

O proximo passo natural e validar esse fluxo com notebooks reais de `inputs/`, cobrindo:

1. leitura do notebook original
2. checkpoints obrigatorios com o usuario
3. revisao estrutural e de sanity checks
4. geracao da versao final em `outputs/`
5. refinamento da skill a partir dos casos de teste
