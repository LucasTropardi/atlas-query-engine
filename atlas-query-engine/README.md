# Atlas Query Engine

Projeto multi-modulo Maven com:

- `atlas-query-engine-core`: pipeline declarativa do engine
- `atlas-query-engine-demo`: aplicacao Spring Boot para laboratorio local

## Subir bancos de laboratorio

Subir apenas o PostgreSQL:

```bash
docker compose up -d atlas-postgres
```

Subir PostgreSQL + MySQL:

```bash
docker compose up -d atlas-postgres atlas-mysql
```

Portas expostas:

- PostgreSQL: `5433`
- MySQL: `3307`

Volumes criados:

- `atlas_postgres_data`
- `atlas_mysql_data`

Derrubar sem apagar volumes:

```bash
docker compose down
```

Derrubar apagando volumes:

```bash
docker compose down -v
```

Oracle nao foi adicionado nesta etapa.

## Rodar a aplicacao

Com o PostgreSQL do compose em execucao:

```bash
./mvnw -pl atlas-query-engine-demo spring-boot:run
```

## Endpoint de laboratorio

`POST /api/query`

Payload de exemplo:

```json
{
  "dataset": "orders",
  "select": ["country"],
  "filters": {
    "operator": "AND",
    "conditions": [
      { "field": "status", "operator": "=", "value": "PAID" },
      {
        "operator": "OR",
        "conditions": [
          { "field": "country", "operator": "=", "value": "BR" },
          { "field": "country", "operator": "=", "value": "US" }
        ]
      }
    ]
  },
  "metrics": [
    { "field": "id", "operation": "count", "alias": "ordersCount" },
    { "field": "amount", "operation": "sum", "alias": "totalAmount" }
  ],
  "groupBy": ["country"],
  "sort": [
    { "field": "totalAmount", "direction": "desc" }
  ],
  "page": 1,
  "pageSize": 50
}
```

O formato legado continua aceitando `filters` como lista simples. Internamente ele e normalizado para um grupo `AND`.

## Contrato de consultas

Informe exatamente um destino: `dataset` ou `table`.

- `dataset`: campos logicos, metricas e relacoes definidos no catalogo. Projecoes,
  expressoes, joins explicitos, `schema`, `alias` e filtros `EXISTS` sao rejeitados
  neste modo, em vez de serem ignorados.
- `table`: consulta direta com schema, alias, joins, projecoes, expressoes e
  `EXISTS`. Ao combinar projecoes com metricas, inclua em `groupBy` todas as
  colunas referenciadas pelas projecoes. Literais nao exigem agrupamento.

Filtros em lista continuam aceitos como entrada e sao convertidos para uma
unica arvore `AND`. A serializacao de `QueryRequest` usa a arvore canonica.
Na API Java, use `setFilters(...)` ou `setFilterTree(...)` para atualizar filtros;
`getFilters()` fornece uma lista de leitura dos filtros simples.

## Catalogo reutilizavel

`new InMemoryDatasetCatalog()` cria um catalogo vazio. Para registrar datasets,
use `new InMemoryDatasetCatalog(definitions)`, passando uma colecao de
`DatasetDefinition`. Nomes duplicados sao rejeitados. Os datasets de laboratorio
ficam em `DemoDatasets`, no modulo demo; o core nao registra exemplos automaticamente.

## Execucao e diagnostico

O demo resolve a conexao uma vez por consulta, usando a mesma configuracao para
selecionar o dialeto e executar o SQL. O cache de datasources e renovado quando
a configuracao da conexao muda. Ele continua usando `DriverManagerDataSource`,
sem pool de conexoes externas.

O engine registra destino, duracao e quantidade de linhas. O SQL com placeholders
fica em DEBUG; parametros e SQL interpolado nao sao registrados pelo engine.

## Testes

```bash
./mvnw test
```

A suite inclui testes de pipeline JSON -> engine -> JDBC com H2, correlacao de
`EXISTS` com `OR`, parametros contendo `$` e barras, validacao por modo e
configuracao Spring. Esses testes nao substituem validacao com bancos reais
PostgreSQL, MySQL e Oracle.

## Descoberta de datasets

```bash
curl http://localhost:8030/api/datasets
curl http://localhost:8030/api/datasets/orders
```

`GET /api/datasets` retorna uma lista de objetos com `name`, ordenada por nome.
`GET /api/datasets/{name}` retorna `name`, `fields` e `metrics`. Cada campo
informa `name`, `type`, `filterable` e `sortable`; cada metrica informa `field`,
`type` e `operations` (por exemplo, `sum` e `avg`). As listas de campos e metricas
sao ordenadas por nome. Dataset inexistente retorna HTTP 404; catalogo vazio retorna `[]`.

A descoberta expoe o contrato logico dos datasets cadastrados, sem credenciais,
configuracao de conexao ou mapeamentos fisicos de tabelas e colunas. Ela nao
consulta o catalogo do banco nem lista tabelas externas.

## Previa de SQL

`POST /api/query/preview` recebe o mesmo JSON de `POST /api/query` e utiliza a
mesma normalizacao, validacao e traducao, mas nao chama o executor JDBC.

```bash
curl -X POST http://localhost:8030/api/query/preview \
  -H 'Content-Type: application/json' \
  -d '{"dataset":"orders","select":["country"],"filters":[{"field":"status","operator":"=","value":"PAID"}]}'
```

Exemplo de resposta com o dialeto PostgreSQL:

```json
{
  "target": "orders",
  "mode": "dataset",
  "dialect": "POSTGRES",
  "sql": "SELECT t0.country AS \"country\" FROM public.orders t0 WHERE t0.status = ? LIMIT 51 OFFSET 0",
  "parameters": [{"position": 1, "type": "String"}],
  "fields": ["country"],
  "relations": []
}
```

- `parameters`: posicao JDBC a partir de 1 e tipo Java do valor apos traducao;
  os valores nao sao retornados. O tipo nao e inferido pelo banco.
- `fields`: campos selecionados e aliases de projecoes e metricas, na ordem do SELECT.
- `relations`: nomes das relacoes resolvidas no modo dataset; tabelas dos joins
  principais no modo direto. Joins internos de `EXISTS` aparecem no SQL, nao nesta lista.

A previa nao executa o SELECT no banco de destino nem valida a existencia das
tabelas. Se `connection` for informada, o cadastro de conexoes ainda e consultado
para validar a conexao e determinar o dialeto. Nao se trata de um EXPLAIN do banco.
Consultas invalidas seguem as mesmas regras da execucao.

## Operadores de nulos

`is null` e `is not null` funcionam nos modos dataset e table, inclusive em grupos
aninhados. No modo direto tambem podem ser aplicados a expressoes e filtros de EXISTS.

```json
{
  "dataset": "orders",
  "select": ["country"],
  "filters": {
    "operator": "AND",
    "conditions": [
      {"field": "country", "operator": "is null"},
      {"field": "status", "operator": "is not null"}
    ]
  }
}
```

Esses operadores aceitam `value` ausente ou `null`; valores nao nulos sao rejeitados.
A comparacao IS NULL/IS NOT NULL nao adiciona parametro JDBC. Literais dentro de
uma expressao continuam parametrizados normalmente. Os demais operadores exigem
`value` nao nulo: `= null` e rejeitado, devendo ser substituido por `is null`.
Os nomes dos operadores nao diferenciam maiusculas e minusculas.

### Integracao Java

`QueryEngine` agora inclui `preview(QueryRequest)`, retornando `QueryPreview`.
Implementacoes proprias dessa interface precisam implementar o novo metodo.
`DatasetCatalog` inclui `findAll()` para descoberta; catalogos personalizados
tambem precisam implementar esse metodo. `InMemoryDatasetCatalog` ja implementa ambos
os acessos ao catalogo, por nome e por listagem.

## NOT IN, DISTINCT, HAVING e paginacao

Os recursos abaixo funcionam nos modos `dataset` e `table`, tanto na previa
quanto na execucao.

### Filtro NOT IN

```json
{"field":"status","operator":"not in","value":["CANCELLED","REFUNDED"]}
```

Assim como `in`, `not in` exige uma colecao nao vazia e parametriza cada valor.
A semantica e a do SQL: NOT IN nao inclui automaticamente linhas cujo campo e
NULL. Para inclui-las, combine o filtro com `is null` em um grupo OR.

### Linhas distintas

Use `"distinct": true` para gerar `SELECT DISTINCT`. A deduplicacao considera a
linha inteira projetada e acontece no banco antes da paginacao.

```json
{
  "dataset": "orders",
  "distinct": true,
  "select": ["country"],
  "sort": [{"field":"country","direction":"asc"}],
  "pageSize": 10
}
```

Com DISTINCT, `sort.field` deve referenciar um campo selecionado ou um alias de
saida (projecao ou metrica). Expressoes em `sort.expression` sao rejeitadas nesse
modo para manter o contrato consistente entre os dialetos.

### Filtrar agregacoes com HAVING

`filters` gera WHERE e filtra registros antes da agregacao. O campo `having`
gera HAVING e filtra os grupos depois da agregacao. Ele aceita filtros simples,
listas (AND implicito) e grupos AND/OR, usando os mesmos operadores de comparacao.

Nesta versao, `having.field` deve ser o alias de uma metrica declarada em
`metrics`. Campos comuns, expressoes arbitrarias e EXISTS em HAVING sao
rejeitados. Sem filtros HAVING, o comportamento anterior e preservado.

```json
{
  "dataset": "orders",
  "select": ["country"],
  "filters": {
    "field": "status",
    "operator": "not in",
    "value": ["CANCELLED"]
  },
  "metrics": [
    {"field":"amount","operation":"sum","alias":"totalAmount"}
  ],
  "groupBy": ["country"],
  "having": {
    "field": "totalAmount",
    "operator": ">",
    "value": 1000
  },
  "sort": [
    {"field":"totalAmount","direction":"desc"},
    {"field":"country","direction":"asc"}
  ],
  "page": 1,
  "pageSize": 10
}
```

O alias `totalAmount` e resolvido para `SUM(t0.amount)` no HAVING, sem depender
do suporte do banco a aliases nessa clausula. Valores continuam parametrizados.
Para comparar metricas numericas, envie numeros JSON. Agregacoes sem GROUP BY
tambem podem usar HAVING, desde que nao selecionem dimensoes nao agrupadas.

### Indicador hasNext

A resposta inclui `metadata.hasNext`. Exemplo ilustrativo de metadados:

```json
{
  "dataset": "orders",
  "executionTimeMs": 8,
  "page": 1,
  "pageSize": 10,
  "rowCount": 10,
  "hasNext": true
}
```

O SQL busca `pageSize + 1` linhas. O executor usa a linha adicional para detectar
uma proxima pagina e a remove da resposta. `rowCount` conta apenas as linhas
retornadas ao cliente, e `pageSize` permanece o tamanho solicitado. O OFFSET
continua sendo `(page - 1) * pageSize`, sem incluir a linha adicional.

- A previa mostra o SQL real, incluindo o limite adicional (por exemplo, LIMIT 11).
- Uma ultima pagina cheia tem `hasNext: false` quando nao existe linha adicional.
- Paginas vazias ou parciais tambem retornam `hasNext: false`.
- O limite publico continua sendo 500 linhas por pagina; o banco pode buscar 501.
- Nao e executado COUNT adicional e nao ha contagem total nesta resposta.
- Offsets que excedem o intervalo de inteiro suportado sao rejeitados.

Para navegar com uma ordem previsivel, informe `sort` com desempate unico: por
exemplo, ID em consultas de registros, ou os campos do agrupamento em relatorios.
O engine nao inventa uma chave unica nem garante estabilidade entre requisicoes
quando os dados mudam no banco.

O executor JDBC fornecido implementa `hasNext`. Implementacoes personalizadas de
`QueryExecutor` precisam respeitar o limite publico da pagina, remover a linha
adicional e preencher o indicador ao consumir o SQL produzido pelos tradutores.
