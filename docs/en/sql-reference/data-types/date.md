---
description: 'Documentation for the Date data type in ClickHouse'
sidebar_label: 'Date'
sidebar_position: 12
slug: /sql-reference/data-types/date
title: 'Date'
---

# Date

A date. Stored in two bytes as the number of days since 1970-01-01 (unsigned). Allows storing values from just after the beginning of the Unix Epoch to the upper threshold defined by a constant at the compilation stage (currently, this is until the year 2149, but the final fully-supported year is 2148).

Supported range of values: \[1970-01-01, 2149-06-06\].

The date value is stored without the time zone.

**Example**

Creating a table with a `Date`-type column and inserting data into it:

```sql
CREATE TABLE dt
(
    `timestamp` Date,
    `event_id` UInt8
)
ENGINE = TinyLog;
```

```sql
-- Parse Date
-- - from string,
-- - from 'small' integer interpreted as number of days since 1970-01-01, and
-- - from 'big' integer interpreted as number of seconds since 1970-01-01.
INSERT INTO dt VALUES ('2019-01-01', 1), (17897, 2), (1546300800, 3);

SELECT * FROM dt;
```

```text
┌──timestamp─┬─event_id─┐
│ 2019-01-01 │        1 │
│ 2019-01-01 │        2 │
│ 2019-01-01 │        3 │
└────────────┴──────────┘
```

**See Also**

- [Functions for working with dates and times](../../sql-reference/functions/date-time-functions.md)
- [Operators for working with dates and times](../../sql-reference/operators#operators-for-working-with-dates-and-times)
- [`DateTime` data type](../../sql-reference/data-types/datetime.md)
