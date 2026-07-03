# vapordb — Limitations

Known limitations and remaining roadmap items. See the [README](README.md) for basics and [FEATURES.md](FEATURES.md) for the full feature set.

## Limitations

- No indexes. All queries do a full table scan.
- No foreign key constraints. Model relations with JOINs.
- MySQL SQL dialect (via `github.com/xwb1989/sqlparser`). Some combinations (for example `LIKE` immediately followed by `||` without parentheses) parse better if you parenthesize the pattern expression. Standard-SQL / PostgreSQL double-quoted identifiers (`"name"`, `"type"`, …) are automatically rewritten to backtick identifiers, and `ILIKE` / `NOT ILIKE` are rewritten to `LOWER(x) LIKE LOWER(y)`, so PostgreSQL-style queries work without changes.

## Roadmap — remaining priorities

- **Window functions inside `INSERT … SELECT`** — window expressions (e.g. `ROW_NUMBER() OVER (…)`, `LAG`, `SUM(…) OVER (…)`) in the projection of an `INSERT … SELECT` are not yet supported, because window extraction is only wired into the `Query` / SELECT path, not the `Exec` / `RETURNING` INSERT paths. Workaround: compute the window result in a CTE and `INSERT … SELECT` from it:

  ```sql
  WITH ranked AS (SELECT id, ROW_NUMBER() OVER (ORDER BY id) AS rn FROM src)
  INSERT INTO dst (id, rn) SELECT id, rn FROM ranked
  ```

- **`INSERT` without an explicit column list** — `INSERT INTO t VALUES (…)` and `INSERT INTO t SELECT …` are rejected; a column list is required (`INSERT INTO t (a, b) …`). The schema is an unordered map, so there is no positional column order to map bare value tuples onto.

- **Scalar subquery with `UNION`** — a scalar subquery used where a single value is expected, e.g. `WHERE x = (SELECT … UNION SELECT …)`, is rejected. Scalar subqueries must be a single `SELECT` returning one column. (Uncorrelated `IN (… UNION …)` and `FROM (… UNION …)` are fine; this only affects the scalar-value position.)

- **`WITH RECURSIVE`** — recursive CTEs are not supported. Non-recursive `WITH` works (it is pre-processed into ordinary virtual tables).

- **Function calls / parentheses in a partial `ON CONFLICT` predicate** — `ON CONFLICT (cols) WHERE predicate DO …` strips the predicate, but the predicate must not contain parentheses or function calls (the rewrite regex stops at the first `(`). Plain column predicates such as `WHERE deleted_at IS NULL` work.

- **Multi-table `UPDATE` / `DELETE`** — `UPDATE`/`DELETE` operate on a single table; join-based multi-table updates/deletes are not supported.
