# CODEX Notes

- **Scope**: `daplug_cypher.common.BaseAdapter` now treats adapter-level `sns_attributes` as defaults that merge with per-call overrides. `operation` is injected automatically; `None` values are stripped and numbers are typed correctly for SNS message attributes.
- **Tests**: Critical coverage lives in `tests/unit/common/test_base_adapter.py`. Run `pipenv run pytest tests/unit/common/test_base_adapter.py` for focused checks or `pipenv run pytest` for the full suite (integration tests skip unless Bolt endpoints exist).
- **Key Modules**:
  - `daplug_cypher/common/base_adapter.py` – SNS helper logic.
  - `daplug_cypher/adapter.py` – graph adapter that calls `BaseAdapter.publish`.
- **Recent Work**: Added override semantics + type inference docs (README) and expanded tests to lock behavior.
- **Usage Tips**: When building new features, inject adapter defaults through `sns_attributes` at construction and rely on per-call overrides to layer request context.
