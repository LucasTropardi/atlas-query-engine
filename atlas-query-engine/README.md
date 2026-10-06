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
