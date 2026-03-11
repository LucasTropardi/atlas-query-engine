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
