---
slug: /zh/sql-reference/data-types/simpleaggregatefunction
---
# SimpleAggregateFunction {#data-type-simpleaggregatefunction}

`SimpleAggregateFunction(name, types_of_arguments...)` 数据类型存储聚合函数的当前值, 并不像 [`AggregateFunction`](../../sql-reference/data-types/aggregatefunction.md) 那样存储其全部状态。这种优化可以应用于具有以下属性函数: 将函数 `f` 应用于行集合 `S1 UNION ALL S2` 的结果，可以通过将 `f` 分别应用于行集合的部分, 然后再将 `f` 应用于结果来获得: `f(S1 UNION ALL S2) = f(f(S1) UNION ALL f(S2))`。 这个属性保证了部分聚合结果足以计算出合并的结果，所以我们不必存储和处理任何额外的数据。

支持以下聚合函数:

-   [`any`](/sql-reference/aggregate-functions/reference/any)
-   [`anyLast`](/sql-reference/aggregate-functions/reference/anylast)
-   [`min`](/sql-reference/aggregate-functions/reference/min)
-   [`max`](/sql-reference/aggregate-functions/reference/max)
-   [`sum`](/sql-reference/aggregate-functions/reference/sum)
-   [`sumWithOverflow`](/sql-reference/aggregate-functions/reference/sumwithoverflow)
-   [`groupBitAnd`](/sql-reference/aggregate-functions/reference/groupbitand)
-   [`groupBitOr`](/sql-reference/aggregate-functions/reference/groupbitor)
-   [`groupBitXor`](/sql-reference/aggregate-functions/reference/groupbitxor)
-   [`groupArrayArray`](/sql-reference/aggregate-functions/reference/grouparray)
-   [`groupUniqArrayArray`](../../sql-reference/aggregate-functions/reference/groupuniqarray.md)
-   [`groupUniqArrayArrayMap`](../../sql-reference/aggregate-functions/combinators#-map)
-   [`sumMap`](/sql-reference/aggregate-functions/reference/summap)
-   [`minMap`](/sql-reference/aggregate-functions/reference/minmap)
-   [`maxMap`](/sql-reference/aggregate-functions/reference/maxmap)
-   [`argMin`](../../sql-reference/aggregate-functions/reference/argmin.md)
-   [`argMax`](../../sql-reference/aggregate-functions/reference/argmax.md)


:::note
`SimpleAggregateFunction(func, Type)` 的值外观和存储方式于 `Type` 相同, 所以你不需要应用带有 `-Merge`/`-State` 后缀的函数。
`SimpleAggregateFunction` 的性能优于具有相同聚合函数的 `AggregateFunction` 。
:::

**参数**

-   聚合函数的名称。
-   聚合函数参数的类型。

**示例**

``` sql
CREATE TABLE simple (id UInt64, val SimpleAggregateFunction(sum, Double)) ENGINE=AggregatingMergeTree ORDER BY id;
```
