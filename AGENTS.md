# Agent Usage Tutorial

Automation agents use this document to learn how to exercise the `daplug-cypher` library inside scripts, notebooks, or integration harnesses. Follow the patterns below to wire up graph workloads quickly.

## 1. Install & Import

```bash
pip install daplug-cypher neo4j boto3
```

```python
from daplug_cypher import adapter
```

## 2. Instantiate an Adapter

Tie the adapter to Bolt-compatible graph targets. Provide Neo4j creds locally, Neptune creds when deploying. `neptune` wins when both supplied.

```python
graph = adapter(
    bolt={"url": "bolt://localhost:7687", "user": "neo4j", "password": "password"},
    neptune={"url": "bolt://prod-neptune:8182", "user": "svc", "password": "secret"},
    schema_file="openapi.yml",
    schema="CustomerModel",
    sns_arn="arn:aws:sns:us-east-2:123456789012:customers",
    sns_attributes={"service": "crm"},
)
```

## 3. Schema-Aware Writes

Pass `schema` when you need JSON-schema normalization. The adapter projects payloads onto the schema and strips extraneous fields before issuing Cypher.

```python
graph.create(
    data={"customer_id": "abc123", "name": "Ada", "version": 1},
    node="Customer",
    schema="CustomerModel",  # optional override per call
)
```

## 4. Reads & Raw Queries

Use `read` for parameterized MATCH statements that should serialize nodes automatically. Use `query` for arbitrary Cypher (returns driver records by default).

```python
customers = graph.read(
    query="MATCH (c:Customer) WHERE c.customer_id = $id RETURN c",
    placeholder={"id": "abc123"},
    node="Customer",
)

raw = graph.query(
    query="MATCH (c:Customer)-[r:PLACED]->(o:Order) WHERE c.customer_id = $id RETURN c, r, o",
    placeholder={"id": "abc123"},
)
```

## 5. Optimistic Updates

Provide `identifier`, `idempotence_key`, and the previously observed version. The adapter reads the node first, merges dictionaries, then enforces version equality in Cypher.

```python
graph.update(
    data={"status": "vip"},
    query="MATCH (c:Customer) WHERE c.customer_id = $id RETURN c",
    placeholder={"id": "abc123"},
    node="Customer",
    identifier="customer_id",
    idempotence_key="version",
    original_idempotence_value=1,
)
```

## 6. Deletes with Safety

`delete` first fetches the node so you can inspect what was removed (and so SNS receives meaningful payloads) before detaching.

```python
deleted = graph.delete(
    delete_identifier="abc123",
    node="Customer",
    identifier="customer_id",
)
```

## 7. Relationships

Use helper methods to guard against malformed queries. Relationship builders enforce edge syntax and destructive clauses.

```python
graph.create_relationship(
    query="""
        MATCH (c:Customer), (o:Order)
        WHERE c.customer_id = $customer AND o.order_id = $order
        CREATE (c)-[:PLACED]->(o)
        RETURN c, o
    """,
    placeholder={"customer": "abc123", "order": "o-789"},
)

graph.delete_relationship(
    query="""
        MATCH (c:Customer)-[r:PLACED]->(o:Order)
        WHERE c.customer_id = $customer AND o.order_id = $order
        DETACH DELETE r
    """,
    placeholder={"customer": "abc123", "order": "o-789"},
)
```

## 8. SNS Fan-Out Patterns

- Adapter-level `sns_attributes` store defaults like `{"service": "crm"}`.
- Pass per-call overrides: `graph.create(..., sns_attributes={"source": "api"})`.
- The adapter auto-injects `operation` and removes `None` values.
- Numbers are tagged as SNS `Number` types; strings as `String`.
- Omit `sns_arn` to disable publishing entirely.

## 9. Multi-Model Adapters

Reuse a single adapter for many labels. Supply `node` (and `identifier` when needed) per operation. This keeps driver pools hot while keeping data segregated through Cypher labels.

```python
graph.create(data={"order_id": "o-1", "version": 1}, node="Order")
graph.create(data={"customer_id": "c-1", "version": 1}, node="Customer")
```

## 10. Validating Runs

- **Unit smoke**: `pipenv run pytest tests/unit/common/test_base_adapter.py`
- **Full matrix**: `pipenv run pytest`
- **Integration (requires Bolt)**: `pipenv run test_neo4j`, `pipenv run test_neptune`

Use these commands after composing new usage snippets to ensure the adapter (and SNS hooks) still behave identically.
