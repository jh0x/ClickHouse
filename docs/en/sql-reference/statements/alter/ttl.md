---
description: 'Documentation for Manipulations with Table TTL'
sidebar_label: 'TTL'
sidebar_position: 44
slug: /sql-reference/statements/alter/ttl
title: 'Manipulations with Table TTL'
---

# Manipulations with Table TTL

:::note
If you are looking for details on using TTL for managing old data, check out the [Manage Data with TTL](/guides/developer/ttl.md) user guide. The docs below demonstrate how to alter or remove an existing TTL rule.
:::

## MODIFY TTL {#modify-ttl}

You can change [table TTL](../../../engines/table-engines/mergetree-family/mergetree.md#mergetree-table-ttl) with a request of the following form:

```sql
ALTER TABLE [db.]table_name [ON CLUSTER cluster] MODIFY TTL ttl_expression;
```

## REMOVE TTL {#remove-ttl}

TTL-property can be removed from table with the following query:

```sql
ALTER TABLE [db.]table_name [ON CLUSTER cluster] REMOVE TTL
```

**Example**

Consider the table with table `TTL`:

```sql
CREATE TABLE table_with_ttl
(
    event_time DateTime,
    UserID UInt64,
    Comment String
)
ENGINE MergeTree()
ORDER BY tuple()
TTL event_time + INTERVAL 3 MONTH
SETTINGS min_bytes_for_wide_part = 0;

INSERT INTO table_with_ttl VALUES (now(), 1, 'username1');

INSERT INTO table_with_ttl VALUES (now() - INTERVAL 4 MONTH, 2, 'username2');
```

Run `OPTIMIZE` to force `TTL` cleanup:

```sql
OPTIMIZE TABLE table_with_ttl FINAL;
SELECT * FROM table_with_ttl FORMAT PrettyCompact;
```
Second row was deleted from table.

```text
┌─────────event_time────┬──UserID─┬─────Comment──┐
│   2020-12-11 12:44:57 │       1 │    username1 │
└───────────────────────┴─────────┴──────────────┘
```

Now remove table `TTL` with the following query:

```sql
ALTER TABLE table_with_ttl REMOVE TTL;
```

Re-insert the deleted row and force the `TTL` cleanup again with `OPTIMIZE`:

```sql
INSERT INTO table_with_ttl VALUES (now() - INTERVAL 4 MONTH, 2, 'username2');
OPTIMIZE TABLE table_with_ttl FINAL;
SELECT * FROM table_with_ttl FORMAT PrettyCompact;
```

The `TTL` is no longer there, so the second row is not deleted:

```text
┌─────────event_time────┬──UserID─┬─────Comment──┐
│   2020-12-11 12:44:57 │       1 │    username1 │
│   2020-08-11 12:44:57 │       2 │    username2 │
└───────────────────────┴─────────┴──────────────┘
```

**See Also**

- More about the [TTL-expression](../../../sql-reference/statements/create/table.md#ttl-expression).
- Modify column [with TTL](/sql-reference/statements/alter/ttl).
