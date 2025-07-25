---
description: 'Documentation on integrating ClickHouse with various third-party systems
  and tools'
sidebar_label: 'Integrations'
sidebar_position: 27
slug: /interfaces/third-party/integrations
title: 'Integration Libraries from Third-party Developers'
---

# Integration libraries from third-party developers

:::note Disclaimer
ClickHouse, Inc. does **not** maintain the tools and libraries listed below and haven't done extensive testing to ensure their quality.
:::

## Infrastructure products {#infrastructure-products}

- Relational database management systems
    - [MySQL](https://www.mysql.com)
        - [mysql2ch](https://github.com/long2ice/mysql2ch)
        - [ProxySQL](https://github.com/sysown/proxysql/wiki/ClickHouse-Support)
        - [clickhouse-mysql-data-reader](https://github.com/Altinity/clickhouse-mysql-data-reader)
        - [horgh-replicator](https://github.com/larsnovikov/horgh-replicator)
    - [PostgreSQL](https://www.postgresql.org)
        - [clickhousedb_fdw](https://github.com/Percona-Lab/clickhousedb_fdw)
        - [infi.clickhouse_fdw](https://github.com/Infinidat/infi.clickhouse_fdw) (uses [infi.clickhouse_orm](https://github.com/Infinidat/infi.clickhouse_orm))
        - [pg2ch](https://github.com/mkabilov/pg2ch)
        - [clickhouse_fdw](https://github.com/adjust/clickhouse_fdw)
    - [MSSQL](https://en.wikipedia.org/wiki/Microsoft_SQL_Server)
        - [ClickHouseMigrator](https://github.com/zlzforever/ClickHouseMigrator)
- Message queues
    - [Kafka](https://kafka.apache.org)
        - [clickhouse_sinker](https://github.com/housepower/clickhouse_sinker) (uses [Go client](https://github.com/ClickHouse/clickhouse-go/))
        - [stream-loader-clickhouse](https://github.com/adform/stream-loader)
- Batch processing
    - [Spark](https://spark.apache.org)
        - [spark-clickhouse-connector](https://github.com/housepower/spark-clickhouse-connector)
- Stream processing
    - [Flink](https://flink.apache.org)
        - [flink-clickhouse-sink](https://github.com/ivi-ru/flink-clickhouse-sink)
- Object storages
    - [S3](https://en.wikipedia.org/wiki/Amazon_S3)
        - [clickhouse-backup](https://github.com/AlexAkulov/clickhouse-backup)
- Container orchestration
    - [Kubernetes](https://kubernetes.io)
        - [clickhouse-operator](https://github.com/Altinity/clickhouse-operator)
- Configuration management
    - [puppet](https://puppet.com)
        - [innogames/clickhouse](https://forge.puppet.com/innogames/clickhouse)
        - [mfedotov/clickhouse](https://forge.puppet.com/mfedotov/clickhouse)
- Monitoring
    - [Graphite](https://graphiteapp.org)
        - [graphouse](https://github.com/ClickHouse/graphouse)
        - [carbon-clickhouse](https://github.com/lomik/carbon-clickhouse)
        - [graphite-clickhouse](https://github.com/lomik/graphite-clickhouse)
        - [graphite-ch-optimizer](https://github.com/innogames/graphite-ch-optimizer) - optimizes staled partitions in [\*GraphiteMergeTree](/engines/table-engines/mergetree-family/graphitemergetree) if rules from [rollup configuration](../../engines/table-engines/mergetree-family/graphitemergetree.md#rollup-configuration) could be applied
    - [Grafana](https://grafana.com/)
        - [clickhouse-grafana](https://github.com/Vertamedia/clickhouse-grafana)
    - [Prometheus](https://prometheus.io/)
        - [clickhouse_exporter](https://github.com/f1yegor/clickhouse_exporter)
        - [PromHouse](https://github.com/Percona-Lab/PromHouse)
        - [clickhouse_exporter](https://github.com/hot-wifi/clickhouse_exporter) (uses [Go client](https://github.com/kshvakov/clickhouse/))
    - [Nagios](https://www.nagios.org/)
        - [check_clickhouse](https://github.com/exogroup/check_clickhouse/)
        - [check_clickhouse.py](https://github.com/innogames/igmonplugins/blob/master/src/check_clickhouse.py)
    - [Zabbix](https://www.zabbix.com)
        - [clickhouse-zabbix-template](https://github.com/Altinity/clickhouse-zabbix-template)
    - [Sematext](https://sematext.com/)
        - [clickhouse integration](https://github.com/sematext/sematext-agent-integrations/tree/master/clickhouse)
- Logging
    - [rsyslog](https://www.rsyslog.com/)
        - [omclickhouse](https://www.rsyslog.com/doc/master/configuration/modules/omclickhouse.html)
    - [fluentd](https://www.fluentd.org)
        - [loghouse](https://github.com/flant/loghouse) (for [Kubernetes](https://kubernetes.io))
    - [logagent](https://www.sematext.com/logagent)
        - [logagent output-plugin-clickhouse](https://sematext.com/docs/logagent/output-plugin-clickhouse/)
- Geo
    - [MaxMind](https://dev.maxmind.com/geoip/)
        - [clickhouse-maxmind-geoip](https://github.com/AlexeyKupershtokh/clickhouse-maxmind-geoip)
- AutoML
    - [MindsDB](https://mindsdb.com/)
        - [MindsDB](https://github.com/mindsdb/mindsdb) - Integrates with ClickHouse, making data from ClickHouse accessible to a diverse range of AI/ML models.

## Programming language ecosystems {#programming-language-ecosystems}

- Python
    - [SQLAlchemy](https://www.sqlalchemy.org)
        - [sqlalchemy-clickhouse](https://github.com/cloudflare/sqlalchemy-clickhouse) (uses [infi.clickhouse_orm](https://github.com/Infinidat/infi.clickhouse_orm))
    - [PyArrow/Pandas](https://pandas.pydata.org)
        - [Ibis](https://github.com/ibis-project/ibis)
- PHP
    - [Doctrine](https://www.doctrine-project.org/)
        - [dbal-clickhouse](https://packagist.org/packages/friendsofdoctrine/dbal-clickhouse)
- R
    - [dplyr](https://db.rstudio.com/dplyr/)
        - [RClickHouse](https://github.com/IMSMWU/RClickHouse) (uses [clickhouse-cpp](https://github.com/artpaul/clickhouse-cpp))
- Java
    - [Hadoop](http://hadoop.apache.org)
        - [clickhouse-hdfs-loader](https://github.com/jaykelin/clickhouse-hdfs-loader) (uses [JDBC](../../sql-reference/table-functions/jdbc.md))
- Scala
    - [Akka](https://akka.io)
        - [clickhouse-scala-client](https://github.com/crobox/clickhouse-scala-client)
- C#
    - [ADO.NET](https://docs.microsoft.com/en-us/dotnet/framework/data/adonet/ado-net-overview)
        - [ClickHouse.Ado](https://github.com/killwort/ClickHouse-Net)
        - [ClickHouse.Client](https://github.com/DarkWanderer/ClickHouse.Client)
        - [ClickHouse.Net](https://github.com/ilyabreev/ClickHouse.Net)
        - [ClickHouse.Net.Migrations](https://github.com/ilyabreev/ClickHouse.Net.Migrations)
        - [Linq To DB](https://github.com/linq2db/linq2db)
- Elixir
    - [Ecto](https://github.com/elixir-ecto/ecto)
        - [clickhouse_ecto](https://github.com/appodeal/clickhouse_ecto)
- Ruby
    - [Ruby on Rails](https://rubyonrails.org/)
        - [activecube](https://github.com/bitquery/activecube)
        - [ActiveRecord](https://github.com/PNixx/clickhouse-activerecord)
    - [GraphQL](https://github.com/graphql)
        - [activecube-graphql](https://github.com/bitquery/activecube-graphql)
