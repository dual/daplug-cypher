# 🔗 daplug-cypher (da•plug)

> **Schema-Driven Cypher Normalization & Event Publishing for Python**

[![CircleCI](https://circleci.com/gh/dual/daplug-cypher.svg?style=shield)](https://circleci.com/gh/dual/daplug-cypher)
[![Python](https://img.shields.io/badge/python-3.9%2B-blue)](https://www.python.org/downloads/)
[![License](https://img.shields.io/badge/license-apache%202.0-blue)](LICENSE)
[![Contributions](https://img.shields.io/badge/contributions-welcome-blue)](https://github.com/paulcruse3/daplug-cypher/issues)
[![PyPI package](https://img.shields.io/pypi/v/daplug-cypher?color=blue&label=pypi%20package)](https://pypi.org/project/daplug-cypher/)
[![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=dual_daplug-cypher&metric=alert_status)](https://sonarcloud.io/summary/new_code?id=dual_daplug-cypher)
[![Bugs](https://sonarcloud.io/api/project_badges/measure?project=dual_daplug-cypher&metric=bugs)](https://sonarcloud.io/summary/new_code?id=dual_daplug-cypher)
[![Coverage](https://sonarcloud.io/api/project_badges/measure?project=dual_daplug-cypher&metric=coverage)](https://sonarcloud.io/summary/new_code?id=dual_daplug-cypher)

`daplug-cypher` brings the ergonomics of  an adapter patthern to graph databases. It bundles Cypher-friendly schema mapping, optimistic concurrency, and SNS event fan-out so your graph services stay DRY, version-safe, and event-driven—whether you deploy to Neo4j or AWS Neptune (openCypher).

## ✨ Key Features

- **Unified factory** – `daplug_cypher.adapter(**kwargs)` returns a ready-to-go adapter with SNS support, just like `daplug_ddb`.
- **Schema mapping** – Reuse OpenAPI/JSON schemas to validate and normalize payloads before writing nodes or relationships.
- **Optimistic concurrency** – Guard updates with identifier + version keys; the adapter enforces atomic Cypher `SET` semantics.
- **Relationship helpers** – Convenience methods that enforce safe Cypher patterns for creating/deleting relationships.
- **Backend flexibility** – Supply `bolt={...}` for Neo4j, `neptune={...}` for Neptune, or both; the adapter chooses the right driver config automatically.
- **Per-operation targeting** – Pass `node`, `identifier`, and `idempotence_key` to each call so shared adapters can manage multiple labels safely.
- **Per-call SNS metadata** – Supply `sns_attributes` when writing to annotate events with request-specific context.

## 🚀 Quick Start

### Installation

```bash
pip install daplug-cypher
# pipenv install daplug-cypher
# poetry add daplug-cypher
# uv pip install daplug-cypher
```

### Basic Usage

```python
from daplug_cypher import adapter

graph = adapter(
    bolt={
        "url": "bolt://localhost:7687",
        "user": "neo4j",
        "password": "password",
    },
    schema_file="openapi.yml",
    schema="CustomerModel",
)

payload = {
    "customer_id": "abc123",
    "name": "Ada",
    "version": 1,
}

graph.create(data=payload, node="Customer")
result = graph.read(
    query="MATCH (c:Customer) WHERE c.customer_id = $id RETURN c",
    placeholder={"id": "abc123"},
    node="Customer",
)

print(result["Customer"][0]["name"])

graph.create(
    data=payload,
    node="Customer",
    sns_attributes={"source": "api"},
)
```

Because the adapter is schema-aware, every write can opt into mapping by passing `schema`. Skip it when you want to persist the payload exactly as provided. Select the node label (and identifiers) per call so a single adapter can service multiple models, and add `sns_attributes` when you want to decorate published events with request-specific context.

## 🔧 Advanced Configuration

### Public API Cheat Sheet

```python
from daplug_cypher import adapter

graph = adapter(bolt={"url": "bolt://localhost:7687", "user": "neo4j", "password": "password"})

# CREATE ---------------------------------------------------------------
graph.create(
    data={"customer_id": "abc123", "name": "Ada", "version": 1},
    node="Customer",
    sns_attributes={"event": "customer-created"},
)

# READ / MATCH ---------------------------------------------------------
graph.read(
    query="MATCH (c:Customer) WHERE c.customer_id = $id RETURN c",
    placeholder={"id": "abc123"},
    node="Customer",
)

# QUERY (raw parameterized Cypher) ------------------------------------
graph.query(
    query="MATCH (c:Customer) WHERE c.customer_id = $id RETURN c",
    placeholder={"id": "abc123"},
    sns_attributes={"source": "reporting"},
)

# UPDATE (optimistic) --------------------------------------------------
graph.update(
    data={"status": "vip"},
    query="MATCH (c:Customer) WHERE c.customer_id = $id RETURN c",
    placeholder={"id": "abc123"},
    original_idempotence_value=1,
    node="Customer",
    identifier="customer_id",
    idempotence_key="version",
    sns_attributes={"event": "customer-updated"},
)

# DELETE ---------------------------------------------------------------
graph.delete(
    delete_identifier="abc123",
    node="Customer",
    identifier="customer_id",
    sns_attributes={"event": "customer-deleted"},
)

# RELATIONSHIP HELPERS -------------------------------------------------
graph.create_relationship(
    query="""
        MATCH (c:Customer), (o:Order)
        WHERE c.customer_id = $customer AND o.order_id = $order
        CREATE (c)-[:PLACED]->(o)
        RETURN c, o
    """,
    placeholder={"customer": "abc123", "order": "o-789"},
    sns_attributes={"event": "relationship-created"},
)

graph.delete_relationship(
    query="""
        MATCH (c:Customer)-[r:PLACED]->(o:Order)
        WHERE c.customer_id = $customer AND o.order_id = $order
        DETACH DELETE r
    """,
    placeholder={"customer": "abc123", "order": "o-789"},
    sns_attributes={"event": "relationship-deleted"},
)
```

Each method mirrors the DynamoDB adapter API: provide per-call metadata, and the adapter handles schema normalization, optimistic locking, driver orchestration, and optional SNS fan-out.

### Neo4j & Neptune Targets

```python
graph = adapter(
    bolt={"url": "bolt://localhost:7687", "user": "neo4j", "password": "password"},
    neptune={"url": "bolt://neptune-endpoint:8182", "user": "user", "password": "secret"},
)
```

Provide both dictionaries to allow local Neo4j development with a production Neptune endpoint. When `neptune` is supplied it wins; otherwise `bolt` is used.
Use the same adapter instance for different node types by passing the appropriate label to each call (e.g., `graph.create(..., node="Order")`).

### Optimistic Updates

```python
graph.update(
    data={"order_id": "abc123", "updated_at": 2, "status": "shipped"},
    query="MATCH (o:Order) WHERE o.order_id = $id RETURN o",
    placeholder={"id": "abc123"},
    original_idempotence_value=1,  # the previous value of updated_at
    node="Order",
    identifier="order_id",
    idempotence_key="updated_at",
    sns_attributes={"event": "status-change"},
)
```

If another session updates the node first, the adapter raises `ValueError("ATOMIC ERROR...")` rather than overwriting silently.

### Relationship Helpers

```python
graph.create(data={"customer_id": "abc123", "version": 1}, node="Customer")
graph.create(data={"order_id": "o-789", "version": 1}, node="Order")

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

Validation ensures relationship queries include edge notation and destructive operations actually delete nodes/relationships.

### SNS Event Publishing

```python
graph = adapter(
    bolt={...},
    sns_arn="arn:aws:sns:us-east-2:123456789012:customers",
    sns_attributes={"service": "crm"},
)

graph.delete(delete_identifier="abc123", node="Customer", identifier="customer_id")
```

Each write publishes an event with merged attributes (`operation`, default adapter attributes, custom attributes, and call-specific overrides).

## 🧪 Testing

We split fast unit tests from integration suites targeting Neo4j and Neptune-compatible endpoints.

```bash
# Unit tests (pure Python, heavy mocking)
pipenv run test

# Integration suites
pipenv run test_neo4j     # requires Neo4j Bolt endpoint (defaults to bolt://localhost:7687)
pipenv run test_neptune   # reuses Bolt settings, can point at Neptune or LocalStack

# Coverage (Neo4j suite under coverage)
pipenv run coverage
```

Environment variables to override defaults:

| Variable               | Purpose                            | Default               |
| ---------------------- | ---------------------------------- | --------------------- |
| `NEO4J_BOLT_URL`       | Neo4j Bolt connection URI          | `bolt://localhost:7687` |
| `NEO4J_USER` / `_PASSWORD` | Neo4j credentials                | `neo4j` / `password`  |
| `NEPTUNE_BOLT_URL`     | Neptune Bolt-compatible endpoint   | falls back to Neo4j   |
| `NEPTUNE_USER` / `_PASSWORD` | Neptune credentials             | falls back to Neo4j   |

## 🧰 Tooling & CI

`.circleci/config.yml` mirrors the DynamoDB project:

- `install-build` installs dependencies and persists the workspace.
- `lint` and `type-check` run `pipenv run lint` and `pipenv run mypy`.
- `test-neo4j` and `test-neptune` run pytest markers in parallel; the Neptune job provisions a LocalStack container for compatibility checks.
- `install-build-publish` retains the token-based PyPI workflow.

## 🛠️ Local Development

### Prerequisites

- Python **3.9+**
- [Pipenv](https://pipenv.pypa.io/)
- Docker (for running Neo4j or LocalStack locally)

### Environment Setup

```bash
git clone https://github.com/paulcruse3/daplug-cypher.git
cd daplug-cypher
pipenv install --dev
```

### Workflow

```bash
pipenv run lint          # pylint (JSON + HTML report)
pipenv run mypy          # static typing (post-phase polish)
pipenv run test          # unit tests
pipenv run test_neo4j    # integration suite (requires Bolt endpoint)
pipenv run test_neptune  # integration suite (LocalStack/Neptune)
```

When running Neo4j via Docker, set `NEO4J_AUTH=neo4j/password` before `docker run` so the tests can authenticate automatically.

## 🗂️ Project Structure

```txt
daplug-cypher/
├── daplug_cypher/
│   ├── adapter.py         # Cypher adapter implementation
│   ├── common/            # Shared schema, merge, logging, publisher helpers
│   ├── cypher/            # Parameter + serialization utilities
│   ├── types/             # Shared TypedDict/type aliases (reused by common)
│   └── __init__.py        # Public adapter factory & exports
├── tests/
│   ├── integration/       # Neo4j & Neptune pytest suites
│   └── unit/              # Mock-based unit coverage for every module
├── .circleci/config.yml   # CI pipeline
├── Pipfile                # Runtime & dev dependencies + scripts
├── setup.py / setup.cfg   # Packaging metadata & pytest config
└── README.md              # You are here
```

## 🤝 Contributing

We welcome issues and pull requests! Please ensure linting, typing, and both integration suites pass before submitting.

```bash
git checkout -b feature/amazing-cypher
# make your changes
pipenv run lint
pipenv run mypy
pipenv run test
pipenv run test_neo4j
pipenv run test_neptune
git commit -am "feat: amazing cypher enhancement"
git push origin feature/amazing-cypher
```

## 📄 License

Apache License 2.0 – see [LICENSE](LICENSE) for the full text.

---

> Built to keep Cypher integrations as clean and predictable
