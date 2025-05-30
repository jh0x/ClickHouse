import logging
import random
import threading
import time
from multiprocessing.dummy import Pool

import pytest

from helpers.client import QueryRuntimeException
from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry

DICTIONARY_FILES = [
    "configs/dictionaries/dep_x.xml",
    "configs/dictionaries/dep_y.xml",
    "configs/dictionaries/dep_z.xml",
    "configs/dictionaries/node.xml",
]

cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance(
    "node1",
    main_configs=["configs/config.xml"],
    dictionaries=DICTIONARY_FILES,
    with_zookeeper=True,
    stay_alive=True,
)

node2 = cluster.add_instance(
    "node2",
    main_configs=[
        "configs/async_load_system_database.xml",
    ],
    dictionaries=DICTIONARY_FILES,
    with_zookeeper=True,
    stay_alive=True,
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()

        nodenum = 1
        for node in [node1, node2]:
            node.query(
                f"""
                CREATE DATABASE IF NOT EXISTS dict ENGINE=Dictionary;
                CREATE DATABASE IF NOT EXISTS test;
                CREATE TABLE test_table_src(date Date, id UInt32, dummy UInt32)
                ENGINE = ReplicatedMergeTree('/clickhouse/tables/test_table', '{nodenum}')
                PARTITION BY date ORDER BY id
                """
            )
            nodenum += 1

        yield cluster

    finally:
        cluster.shutdown()


def get_status(dictionary_name):
    return node1.query(
        "SELECT status FROM system.dictionaries WHERE name='" + dictionary_name + "'"
    ).rstrip("\n")


def test_dict_get_data(started_cluster):
    query = node1.query

    query(
        "CREATE TABLE test.elements (id UInt64, a String, b Int32, c Float64) ENGINE=Log;"
    )
    query(
        "INSERT INTO test.elements VALUES (0, 'water', 10, 1), (1, 'air', 40, 0.01), (2, 'earth', 100, 1.7);"
    )

    # dictionaries_lazy_load == false, so these dictionary are not loaded.
    assert get_status("dep_x") == "NOT_LOADED"
    assert get_status("dep_y") == "NOT_LOADED"
    assert get_status("dep_z") == "NOT_LOADED"

    # Dictionary 'dep_x' depends on 'dep_z', which depends on 'dep_y'.
    # So they all should be loaded at once.
    assert query("SELECT dictGetString('dep_x', 'a', toUInt64(1))") == "air\n"
    assert get_status("dep_x") == "LOADED"
    assert get_status("dep_y") == "LOADED"
    assert get_status("dep_z") == "LOADED"

    # Other dictionaries should work too.
    assert query("SELECT dictGetString('dep_y', 'a', toUInt64(1))") == "air\n"
    assert query("SELECT dictGetString('dep_z', 'a', toUInt64(1))") == "air\n"

    assert query("SELECT dictGetString('dep_x', 'a', toUInt64(3))") == "XX\n"
    assert query("SELECT dictGetString('dep_y', 'a', toUInt64(3))") == "YY\n"
    assert query("SELECT dictGetString('dep_z', 'a', toUInt64(3))") == "ZZ\n"

    # Update the source table.
    query("INSERT INTO test.elements VALUES (3, 'fire', 30, 8)")

    # Wait for dictionaries to be reloaded.
    assert_eq_with_retry(
        node1,
        "SELECT dictHas('dep_x', toUInt64(3))",
        "1",
        sleep_time=2,
        retry_count=10,
    )
    assert query("SELECT dictGetString('dep_x', 'a', toUInt64(3))") == "fire\n"
    assert query("SELECT dictGetString('dep_y', 'a', toUInt64(3))") == "fire\n"
    assert query("SELECT dictGetString('dep_z', 'a', toUInt64(3))") == "fire\n"

    # dep_z (and hence dep_x) are updated only when there `intDiv(count(), 4)` is changed, now `count()==4`,
    # so dep_x and dep_z are not going to be updated after the following INSERT.
    query("INSERT INTO test.elements VALUES (4, 'ether', 404, 0.001)")
    assert_eq_with_retry(
        node1,
        "SELECT dictHas('dep_y', toUInt64(4))",
        "1",
        sleep_time=2,
        retry_count=10,
    )
    assert query("SELECT dictGetString('dep_x', 'a', toUInt64(4))") == "XX\n"
    assert query("SELECT dictGetString('dep_y', 'a', toUInt64(4))") == "ether\n"
    assert query("SELECT dictGetString('dep_z', 'a', toUInt64(4))") == "ZZ\n"
    query("DROP TABLE IF EXISTS test.elements;")
    node1.restart_clickhouse()


def dependent_tables_assert():
    res = node1.query("select database || '.' || name from system.tables")
    assert "system.join" in res
    assert "default.src" in res
    assert "dict.dep_y" in res
    assert "lazy.log" in res
    assert "test.d" in res
    assert "default.join" in res
    assert "a.t" in res


def test_dependent_tables(started_cluster):
    query = node1.query
    query("create database lazy engine=Lazy(10)")
    query("create database a")
    query("create table lazy.src (n int, m int) engine=Log")
    query(
        "create dictionary a.d (n int default 0, m int default 42) primary key n "
        "source(clickhouse(host 'localhost' port tcpPort() user 'default' table 'src' password '' db 'lazy'))"
        "lifetime(min 1 max 10) layout(flat())"
    )
    query("create table system.join (n int, m int) engine=Join(any, left, n)")
    query("insert into system.join values (1, 1)")
    for i in range(2, 100):
        query(f"insert into system.join values (1, {i})")

    query(
        "create table src (n int, m default joinGet('system.join', 'm', 1::int),"
        "t default dictGetOrNull('a.d', 'm', toUInt64(3)),"
        "k default dictGet('a.d', 'm', toUInt64(4))) engine=MergeTree order by n"
    )
    query(
        "create dictionary test.d (n int default 0, m int default 42) primary key n "
        "source(clickhouse(host 'localhost' port tcpPort() user 'default' table 'src' password '' db 'default'))"
        "lifetime(min 1 max 10) layout(flat())"
    )
    query(
        "create table join (n int, m default dictGet('a.d', 'm', toUInt64(3)),"
        "k default dictGet('test.d', 'm', toUInt64(0))) engine=Join(any, left, n)"
    )
    query(
        "create table lazy.log (n default dictGet(test.d, 'm', toUInt64(0))) engine=Log"
    )
    query(
        "create table a.t (n default joinGet('system.join', 'm', 1::int),"
        "m default dictGet('test.d', 'm', toUInt64(3)),"
        "k default joinGet(join, 'm', 1::int)) engine=MergeTree order by n"
    )

    dependent_tables_assert()
    node1.restart_clickhouse()
    dependent_tables_assert()
    query("drop table a.t")
    query("drop table lazy.log")
    query("drop table join")
    query("drop dictionary test.d")
    query("drop table src")
    query("drop table system.join")
    query("drop database a")
    query("drop database lazy")


def test_multiple_tables(started_cluster):
    query = node1.query
    tables_count = 20
    for i in range(tables_count):
        query(
            f"create table test.table_{i} (n UInt64, s String) engine=MergeTree order by n as select number, randomString(100) from numbers(100)"
        )

    node1.restart_clickhouse()

    order = [i for i in range(tables_count)]
    random.shuffle(order)
    for i in order:
        assert query(f"select count() from test.table_{i}") == "100\n"
    for i in range(tables_count):
        query(f"drop table test.table_{i} sync")


def test_async_load_system_database(started_cluster):
    id = 1
    for i in range(4):
        # Access some system tables that might be still loading
        if id > 1:
            for j in range(3):
                node2.query(
                    f"select count() from system.text_log_{random.randint(1, id - 1)}_test"
                )
                node2.query(
                    f"select count() from system.query_log_{random.randint(1, id - 1)}_test"
                )

            assert (
                int(
                    node2.query(
                        f"select count() from system.asynchronous_loader where job ilike '%_log_%_test' and execution_pool = 'BackgroundLoad'"
                    )
                )
                > 0
            )

        # Generate more system tables
        for j in range(10):
            while True:
                node2.query("system flush logs")
                count = int(
                    node2.query(
                        "select count() from system.tables where database = 'system' and name in ['query_log', 'text_log']"
                    )
                )
                if count == 2:
                    break
                time.sleep(0.1)
            node2.query(f"rename table system.text_log to system.text_log_{id}_test")
            node2.query(f"rename table system.query_log to system.query_log_{id}_test")
            id += 1

        # Trigger async load of system database
        node2.restart_clickhouse()

    for i in range(id - 1):
        node2.query(f"drop table if exists system.text_log_{i + 1}_test")
        node2.query(f"drop table if exists system.query_log_{i + 1}_test")


def test_materialized_views(started_cluster):
    query = node1.query
    query("create database test_mv")
    query("create table test_mv.t (Id UInt64) engine=MergeTree order by Id")
    query("create table test_mv.a (Id UInt64) engine=MergeTree order by Id")
    query("create table test_mv.z (Id UInt64) engine=MergeTree order by Id")
    query("create materialized view t_to_a to test_mv.a as select Id from test_mv.t")
    query("create materialized view t_to_z to test_mv.z as select Id from test_mv.t")

    node1.restart_clickhouse()
    query("insert into test_mv.t values(42)")
    assert query("select * from test_mv.a Format CSV") == "42\n"
    assert query("select * from test_mv.z Format CSV") == "42\n"

    query("drop view t_to_a")
    query("drop view t_to_z")
    query("drop table test_mv.t")
    query("drop table test_mv.a")
    query("drop table test_mv.z")
    query("drop database test_mv")


def test_materialized_views_cascaded(started_cluster):
    query = node1.query
    query("create database test_mv")
    query("create table test_mv.t (Id UInt64) engine=MergeTree order by Id")
    query("create table test_mv.a (Id UInt64) engine=MergeTree order by Id")
    query("create table test_mv.z (Id UInt64) engine=MergeTree order by Id")
    query("create materialized view t_to_a to test_mv.a as select Id from test_mv.t")
    query("create materialized view a_to_z to test_mv.z as select Id from test_mv.a")

    node1.restart_clickhouse()
    query("insert into test_mv.t values(42)")
    assert query("select * from test_mv.a Format CSV") == "42\n"
    assert query("select * from test_mv.z Format CSV") == "42\n"

    query("drop view t_to_a")
    query("drop view a_to_z")
    query("drop table test_mv.t")
    query("drop table test_mv.a")
    query("drop table test_mv.z")
    query("drop database test_mv")


def test_materialized_views_cascaded_multiple(started_cluster):
    query = node1.query
    query("create database test_mv")
    query("create table test_mv.t (Id UInt64) engine=MergeTree order by Id")
    query("create table test_mv.a (Id UInt64) engine=MergeTree order by Id")
    query("create table test_mv.x (IdText String) engine=MergeTree order by IdText")
    query(
        "create table to_join (Id UInt64, IdBy100 UInt64) engine=MergeTree order by Id"
    )  # default DB
    query(
        "create table test_mv.z (Id UInt64, IdTextLength UInt64) engine=MergeTree order by Id"
    )
    query(
        "create table test_mv.zz (Id UInt64, IdBy100 UInt64) engine=MergeTree order by Id"
    )
    query(
        "create materialized view t_to_a to test_mv.a as select Id from test_mv.t"
    )  # default DB
    query(
        "create materialized view t_to_x to test_mv.x as select toString(Id) as IdText from test_mv.t"
    )
    query(
        "create materialized view ax_to_z to test_mv.z as select Id, (select max(length(IdText)) from test_mv.x) as IdTextLength from test_mv.a"
    )
    query(
        "create materialized view test_mv.join_to_zz to test_mv.zz as select Id, IdBy100 from test_mv.t INNER JOIN to_join USING(Id);"
    )

    query("insert into to_join values(42, 4200)")

    node1.restart_clickhouse()
    query("insert into test_mv.t values(42)")
    assert query("select * from test_mv.a Format CSV") == "42\n"
    assert query("select * from test_mv.x Format CSV") == '"42"\n'
    assert query("select * from test_mv.z Format CSV") == "42,2\n"
    assert query("select * from test_mv.zz Format CSV") == "42,4200\n"

    query("drop view t_to_a sync")
    query("drop view t_to_x sync")
    query("drop view ax_to_z sync")
    query("drop view test_mv.join_to_zz")
    query("drop table test_mv.t sync")
    query("drop table test_mv.a sync")
    query("drop table test_mv.x sync")
    query("drop table test_mv.z sync")
    query("drop table test_mv.zz sync")
    query("drop table to_join sync")
    query("drop database test_mv sync")


def test_materialized_views_replicated(started_cluster):
    nodenum = 1
    for node in [node1, node2]:
        node.query(
            f"""
            CREATE DATABASE test_mv;
            CREATE TABLE test_mv.test_table_H(id UInt32)
            ENGINE = ReplicatedMergeTree('/clickhouse/tables/test_table_H', '{nodenum}')
            ORDER BY id;
            CREATE TABLE test_mv.test_table_S(id UInt32)
            ENGINE = ReplicatedMergeTree('/clickhouse/tables/test_table_S', '{nodenum}')
            ORDER BY id;
            CREATE MATERIALIZED VIEW test_mv.test_mv TO test_mv.test_table_S
            AS SELECT id FROM test_mv.test_table_H;
            """
        )
        nodenum += 1

    for i in range(100):
        node1.query(
            f"INSERT INTO test_mv.test_table_H VALUES({i}) "
        )

    node1.stop_clickhouse()

    disconnect_event = threading.Event()
    p = Pool(1)

    def reload_node(node, event):
        node.restart_clickhouse()
        logging.debug("CH started, setting event")
        event.set()
    job = p.apply_async(
        reload_node,
        (
            node1,
            disconnect_event,
        )
    )

    # start INSERTS when CH is not started yet
    for i in range(100, 130):
        try:
            node1.query(
                f"INSERT INTO test_mv.test_table_H VALUES({i})"
            )
            logging.debug(f"{i} inserted")
        except QueryRuntimeException as e:
            # CH is not started yet
            logging.debug(f"{i} is not inserted - skip")
            time.sleep(0.2)


    disconnect_event.wait(90)

    for i in range(2000, 2100):
        node1.query(
            f"INSERT INTO test_mv.test_table_H VALUES({i})"
        )

    src_rows = node1.query("select count(*) from test_mv.test_table_H Format CSV")
    logging.debug(f"{src_rows} are found in test_mv.test_table_H (src)")

    job.wait()
    p.close()
    p.join()

    assert (
        node1.query("select count(*) from test_mv.test_table_S Format CSV") == src_rows
    )

    for node in [node1, node2]:
        node.query(
            f"""
            DROP TABLE test_mv.test_table_H SYNC;
            DROP TABLE test_mv.test_table_S SYNC;
            DROP VIEW test_mv.test_mv SYNC;
            DROP DATABASE test_mv SYNC;
            """
        )
