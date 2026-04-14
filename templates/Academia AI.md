**ACADEMIA AI**

1. **Problema:**   
   1. Rotineiramente os business analysts precisam gerar notebooks databricks pra fazer análises e chegar em conclusões de negócios  
   2. Porém, deixar esses notebooks em níveis bons de qualidade exige tempo e esforço  
   3. Notebooks são compartilhados e necessitam de padrão para serem entendíveis entre os diferentes business analysts  
   4. As vezes pegamos o notebook de alguém e não sabemos oq a pessoa fez, então gastamos tempo para entender e conseguir revisar ou complementar  
2. **Boas práticas de um notebook [aqui](https://nubank.slack.com/archives/C0ASRCLFD37/p1776184854582449?thread_ts=1776184850.416059&cid=C0ASRCLFD37):**  
   1. Um bom código SCALA/SPARK  
   2. **Organização dos parâmetros:**  
      1. Bem estruturado  
      2. Como nomear variáveis/colunas  
      3. Sources organizadas  
      4. Income segments e filtros declarados  
      5. Começar com todos os Imports la em cima  
      6. Datasets que vão ser utilizados, declarados  
      7. Minimamente documentado  
      8. Otimizar os códigos para rodar/performar  
   3. **Checks de qualidade:**  
      1. Linhas duplicadas na queries  
      2. Em queries count /count distinct pra ver se esta aparencendo, contagem de nulos  
      3. Verificar se perdemos clientes (usar left joins para evitar inner joins)

3. **Solução:**   
   1. Uma skill que padroniza os notebooks  
   2. 3 pastas quebradas Input/Output e template