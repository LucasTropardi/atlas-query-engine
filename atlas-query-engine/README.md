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
  "select": ["country", "status"],
  "filters": [
    { "field": "status", "operator": "=", "value": "PAID" },
    { "field": "createdAt", "operator": ">=", "value": "2026-01-01" }
  ],
  "metrics": [
    { "field": "id", "operation": "count", "alias": "ordersCount" },
    { "field": "amount", "operation": "sum", "alias": "totalAmount" }
  ],
  "groupBy": ["country", "status"],
  "sort": [
    { "field": "totalAmount", "direction": "desc" }
  ],
  "page": 1,
  "pageSize": 50
}
```
