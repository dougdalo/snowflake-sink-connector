# Relatório de alterações – Cache de metadados

## Contexto geral
- Todas as tasks do conector consultavam o Snowflake em cada inicialização usando `DatabaseMetaData.getColumns()` para descobrir a estrutura das tabelas alvo.
- As listas de colunas eram reutilizadas durante o `flush` para gerar os arquivos CSV, montar `COPY INTO`, `MERGE` e `DELETE`.
- Em clusters com criação frequente de conectores, o custo dessas chamadas crescia linearmente.
- A mudança introduz um cache em memória por JVM, evitando chamadas repetidas para o mesmo par `schema.tabela` dentro do mesmo processo.

---

## Conector v3 (`br.com.datastreambrasil.v3`)

### Antes
- `AbstractProcessor.configMetadata()` chamava `getColumnsFromMetadata()` para a tabela final e para a tabela `_INGEST`.
- Cada chamada abria um `ResultSet` via JDBC e removia as colunas configuradas em `ignore_columns` antes de devolver o resultado.
- No `flush`, `columnsIngestTable` definia a ordem das colunas do CSV e do `COPY INTO` da tabela `_INGEST`; `columnsFinalTable` alimentava as instruções `MERGE` e `DELETE` contra a tabela destino.
- Como não havia cache, cada start/restart da task repetia duas consultas de metadata no Snowflake.

### Depois
- `configMetadata()` agora chama `resolveColumns(...)`, que busca as colunas pelo utilitário compartilhado `MetadataCache` e aplica os filtros (`ignore_columns`) localmente.
- O método `fetchColumnsFromMetadata()` só executa a consulta JDBC se o cache estiver vazio para aquele `schema.tabela`.
- A lista devolvida é imutável e mantém deduplicação para preservar a ordem do Snowflake.
- No `flush` nada muda funcionalmente: as listas continuam guiando a composição do CSV, do `COPY INTO`, do `MERGE` e do `DELETE`. A diferença é que, após a primeira leitura, as próximas tasks reutilizam o resultado em memória.

### Impacto no Snowflake
- Redução imediata de chamadas `DatabaseMetaData.getColumns()` para cada tabela final/_INGEST dentro da mesma JVM.
- Apenas a primeira task que inicializa após o start do processo consulta o Snowflake; as demais usam o cache e iniciam mais rápido.
- As consultas ainda acontecem quando uma tabela é acessada pela primeira vez em um novo worker ou após reiniciar o processo (cache em memória).

---

## Conector v2 (`br.com.datastreambrasil.v2`)

### Antes
- `SnowflakeSinkTask.start()` abria a conexão JDBC e chamava `getColumnsFromMetadata()` para preencher `columnsFinalTable`.
- O método fazia uma consulta ao metadata do Snowflake, convertia os nomes de coluna para maiúsculas e removia itens de `ignore_columns`.
- Durante o `flush`, a lista era usada para ordenar as colunas do CSV em memória e montar o comando `COPY INTO` da tabela final.
- Cada criação/restart de task executava novamente a consulta `getColumns()` no Snowflake.

### Depois
- O carregamento é feito por `resolveColumnsFromMetadata()`, que delega ao `MetadataCache` e aplica filtros (`ignore_columns`) sobre o resultado cacheado.
- `fetchColumnsFromMetadata()` só consulta o JDBC quando o cache não tem entrada para o par `schema.tabela` e continua retornando os nomes em maiúsculas.
- A lista retornada é imutável e mantém a mesma ordem observada no metadata.
- O uso no `flush` permanece inalterado: a lista define a ordem do CSV e compõe o `COPY INTO`, porém reaproveitando o metadata em memória após a primeira leitura por processo.

### Impacto no Snowflake
- Diminuição do número de consultas `DatabaseMetaData.getColumns()` por tabela final a partir da segunda task iniciada na mesma JVM.
- Menor tempo de start para tasks subsequentes, já que a descoberta de colunas passa a ser local.
- Assim como na v3, qualquer alteração de schema exige reinicializar o processo ou limpar o cache para refletir a nova estrutura.

---

## Considerações gerais
- O cache é isolado por processo/JVM: múltiplos workers ainda fazem uma consulta inicial cada.
- A política de invalidação continua sendo reiniciar o worker (não há limpeza automática no código atual).
- Foram adicionados testes unitários para garantir que apenas a primeira tarefa aciona o metadata do Snowflake e que os filtros de colunas permanecem corretos em v2 e v3.
