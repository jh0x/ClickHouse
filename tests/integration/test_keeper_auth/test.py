import time

import pytest
from kazoo.exceptions import (
    AuthFailedError,
    InvalidACLError,
    KazooException,
    NoAuthError,
)
from kazoo.security import make_acl

from helpers import keeper_utils
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    main_configs=["configs/keeper_config.xml"],
    with_zookeeper=True,
    use_keeper=False,
    stay_alive=True,
)

SUPERAUTH = "super:admin"


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        keeper_utils.wait_until_connected(cluster, node)

        yield cluster

    finally:
        cluster.shutdown()


def get_fake_zk(timeout=30.0):
    return keeper_utils.get_fake_zk(cluster, "node", timeout=timeout)


def get_genuine_zk():
    print("Zoo1", cluster.get_instance_ip("zoo1"))
    return cluster.get_kazoo_client("zoo1")


# FIXME: this sleep is a workaround for the bug that is fixed by this patch [1].
#
# The problem is that after AUTH_FAILED (that is caused by the line above)
# there can be a race, because of which, stop() will hang indefinitely.
#
#    [1]: https://github.com/python-zk/kazoo/pull/688
def zk_auth_failure_workaround():
    time.sleep(2)


def zk_stop_and_close(zk):
    if zk:
        zk.stop()
        zk.close()


@pytest.mark.parametrize(("get_zk"), [get_genuine_zk, get_fake_zk])
def test_remove_acl(started_cluster, get_zk):
    auth_connection = None

    try:
        auth_connection = get_zk()

        auth_connection.add_auth("digest", "user1:password1")

        # Consistent with zookeeper, accept generated digest
        auth_connection.create(
            "/test_remove_acl1",
            b"dataX",
            acl=[
                make_acl(
                    "digest",
                    "user1:XDkd2dsEuhc9ImU3q8pa8UOdtpI=",
                    read=True,
                    write=False,
                    create=False,
                    delete=False,
                    admin=False,
                )
            ],
            ephemeral=True,
        )
        auth_connection.create(
            "/test_remove_acl2",
            b"dataX",
            acl=[
                make_acl(
                    "digest",
                    "user1:XDkd2dsEuhc9ImU3q8pa8UOdtpI=",
                    read=True,
                    write=True,
                    create=False,
                    delete=False,
                    admin=False,
                )
            ],
            ephemeral=True,
        )
        auth_connection.create(
            "/test_remove_acl3",
            b"dataX",
            acl=[make_acl("digest", "user1:XDkd2dsEuhc9ImU3q8pa8UOdtpI=", all=True)],
            ephemeral=True,
        )

        auth_connection.delete("/test_remove_acl2")

        auth_connection.create(
            "/test_remove_acl4",
            b"dataX",
            acl=[
                make_acl(
                    "digest",
                    "user1:XDkd2dsEuhc9ImU3q8pa8UOdtpI=",
                    read=True,
                    write=True,
                    create=True,
                    delete=False,
                    admin=False,
                )
            ],
            ephemeral=True,
        )

        acls, stat = auth_connection.get_acls("/test_remove_acl3")

        assert stat.aversion == 0
        assert len(acls) == 1
        for acl in acls:
            assert acl.acl_list == ["ALL"]
            assert acl.perms == 31
    finally:
        zk_stop_and_close(auth_connection)


@pytest.mark.parametrize(("get_zk"), [get_genuine_zk, get_fake_zk])
def test_digest_auth_basic(started_cluster, get_zk):
    try:
        auth_connection = None
        no_auth_connection = None

        auth_connection = get_zk()
        auth_connection.add_auth("digest", "user1:password1")

        auth_connection.create(
            "/test_no_acl",
            b"",
            ephemeral=True,
        )
        auth_connection.create(
            "/test_all_acl",
            b"data",
            acl=[make_acl("auth", "", all=True)],
            ephemeral=True,
        )
        # Consistent with zookeeper, accept generated digest
        auth_connection.create(
            "/test_all_digest_acl",
            b"dataX",
            acl=[make_acl("digest", "user1:XDkd2dsEuhc9ImU3q8pa8UOdtpI=", all=True)],
            ephemeral=True,
        )

        assert auth_connection.get("/test_all_acl")[0] == b"data"
        assert auth_connection.get("/test_all_digest_acl")[0] == b"dataX"

        no_auth_connection = get_zk()
        no_auth_connection.set("/test_no_acl", b"hello")
        assert no_auth_connection.get("/test_no_acl")[0] == b"hello"

        # no ACL, so cannot access these nodes
        with pytest.raises(NoAuthError):
            no_auth_connection.set("/test_all_acl", b"hello")

        with pytest.raises(NoAuthError):
            no_auth_connection.get("/test_all_acl")

        with pytest.raises(NoAuthError):
            no_auth_connection.get("/test_all_digest_acl")

        # still doesn't help
        with pytest.raises(AuthFailedError):
            no_auth_connection.add_auth("world", "anyone")

        zk_auth_failure_workaround()
        zk_stop_and_close(no_auth_connection)
        # session became broken, reconnect
        no_auth_connection = get_zk()

        # wrong auth
        no_auth_connection.add_auth("digest", "user2:password2")

        with pytest.raises(NoAuthError):
            no_auth_connection.set("/test_all_acl", b"hello")

        with pytest.raises(NoAuthError):
            no_auth_connection.set("/test_all_acl", b"hello")

        with pytest.raises(NoAuthError):
            no_auth_connection.get("/test_all_acl")

        with pytest.raises(NoAuthError):
            no_auth_connection.get("/test_all_digest_acl")

        # but can access some non restricted nodes
        no_auth_connection.create(
            "/some_allowed_node",
            b"data",
            ephemeral=True,
        )

        # auth added, go on
        no_auth_connection.add_auth("digest", "user1:password1")
        for path in ["/test_no_acl", "/test_all_acl"]:
            no_auth_connection.set(path, b"auth_added")
            assert no_auth_connection.get(path)[0] == b"auth_added"
    finally:
        zk_stop_and_close(auth_connection)
        zk_stop_and_close(no_auth_connection)


def test_super_auth(started_cluster):
    auth_connection = get_fake_zk()
    try:
        auth_connection.add_auth("digest", "user1:password1")
        auth_connection.create(
            "/test_super_no_acl",
            b"",
            ephemeral=True,
        )
        auth_connection.create(
            "/test_super_all_acl",
            b"data",
            acl=[make_acl("auth", "", all=True)],
            ephemeral=True,
        )

        super_connection = get_fake_zk()
        super_connection.add_auth("digest", "super:admin")
        for path in ["/test_super_no_acl", "/test_super_all_acl"]:
            super_connection.set(path, b"value")
            assert super_connection.get(path)[0] == b"value"
    finally:
        zk_stop_and_close(auth_connection)
        zk_stop_and_close(super_connection)


@pytest.mark.parametrize(("get_zk"), [get_genuine_zk, get_fake_zk])
def test_digest_auth_multiple(started_cluster, get_zk):
    auth_connection = None
    one_auth_connection = None
    other_auth_connection = None

    try:
        auth_connection = get_zk()
        auth_connection.add_auth("digest", "user1:password1")
        auth_connection.add_auth("digest", "user2:password2")
        auth_connection.add_auth("digest", "user3:password3")

        auth_connection.create(
            "/test_multi_all_acl",
            b"data",
            acl=[make_acl("auth", "", all=True)],
            ephemeral=True,
        )

        one_auth_connection = get_zk()
        one_auth_connection.add_auth("digest", "user1:password1")

        one_auth_connection.set("/test_multi_all_acl", b"X")
        assert one_auth_connection.get("/test_multi_all_acl")[0] == b"X"

        other_auth_connection = get_zk()
        other_auth_connection.add_auth("digest", "user2:password2")

        other_auth_connection.set("/test_multi_all_acl", b"Y")

        assert other_auth_connection.get("/test_multi_all_acl")[0] == b"Y"
    finally:
        zk_stop_and_close(auth_connection)
        zk_stop_and_close(one_auth_connection)
        zk_stop_and_close(other_auth_connection)


@pytest.mark.parametrize(("get_zk"), [get_genuine_zk, get_fake_zk])
def test_partial_auth(started_cluster, get_zk):
    auth_connection = get_zk()
    try:
        auth_connection.add_auth("digest", "user1:password1")

        auth_connection.create(
            "/test_partial_acl",
            b"data",
            acl=[
                make_acl(
                    "auth",
                    "",
                    read=False,
                    write=True,
                    create=True,
                    delete=True,
                    admin=True,
                )
            ],
        )

        auth_connection.set("/test_partial_acl", b"X")
        auth_connection.create(
            "/test_partial_acl/subnode",
            b"X",
            acl=[
                make_acl(
                    "auth",
                    "",
                    read=False,
                    write=True,
                    create=True,
                    delete=True,
                    admin=True,
                )
            ],
        )

        with pytest.raises(NoAuthError):
            auth_connection.get("/test_partial_acl")

        with pytest.raises(NoAuthError):
            auth_connection.get_children("/test_partial_acl")

        # exists works without read perm
        assert auth_connection.exists("/test_partial_acl") is not None

        auth_connection.create(
            "/test_partial_acl_create",
            b"data",
            acl=[
                make_acl(
                    "auth",
                    "",
                    read=True,
                    write=True,
                    create=False,
                    delete=True,
                    admin=True,
                )
            ],
            ephemeral=True,
        )
        with pytest.raises(NoAuthError):
            auth_connection.create(
                "/test_partial_acl_create/subnode",
            )

        auth_connection.create(
            "/test_partial_acl_set",
            b"data",
            acl=[
                make_acl(
                    "auth",
                    "",
                    read=True,
                    write=False,
                    create=True,
                    delete=True,
                    admin=True,
                )
            ],
            ephemeral=True,
        )
        with pytest.raises(NoAuthError):
            auth_connection.set("/test_partial_acl_set", b"X")

        # not allowed to delete child node
        auth_connection.create(
            "/test_partial_acl_delete",
            b"data",
            acl=[
                make_acl(
                    "auth",
                    "",
                    read=True,
                    write=True,
                    create=True,
                    delete=False,
                    admin=True,
                )
            ],
        )
        auth_connection.create(
            "/test_partial_acl_delete/subnode",
        )
        with pytest.raises(NoAuthError):
            auth_connection.delete("/test_partial_acl_delete/subnode")
    finally:
        auth_connection.delete("/test_partial_acl/subnode")
        auth_connection.delete("/test_partial_acl")
        acl = make_acl(
            "auth",
            "",
            read=True,
            write=True,
            create=True,
            delete=True,
            admin=True,
        )
        auth_connection.set_acls("/test_partial_acl_delete", acls=[acl])
        auth_connection.set_acls("/test_partial_acl_delete/subnode", acls=[acl])
        auth_connection.delete("/test_partial_acl_delete/subnode")
        auth_connection.delete("/test_partial_acl_delete")
        zk_stop_and_close(auth_connection)


def test_bad_auth_1(started_cluster):
    auth_connection = get_fake_zk()
    with pytest.raises(AuthFailedError):
        auth_connection.add_auth("world", "anyone")
    zk_auth_failure_workaround()
    zk_stop_and_close(auth_connection)


def test_bad_auth_2(started_cluster):
    auth_connection = get_fake_zk()
    with pytest.raises(AuthFailedError):
        print("Sending 1")
        auth_connection.add_auth("adssagf", "user1:password1")
    zk_auth_failure_workaround()
    zk_stop_and_close(auth_connection)


def test_bad_auth_3(started_cluster):
    auth_connection = get_fake_zk()
    with pytest.raises(AuthFailedError):
        print("Sending 2")
        auth_connection.add_auth("digest", "")
    zk_auth_failure_workaround()
    zk_stop_and_close(auth_connection)


def test_bad_auth_4(started_cluster):
    auth_connection = get_fake_zk()
    with pytest.raises(AuthFailedError):
        print("Sending 3")
        auth_connection.add_auth("", "user1:password1")
    zk_auth_failure_workaround()
    zk_stop_and_close(auth_connection)


def test_bad_auth_5(started_cluster):
    auth_connection = get_fake_zk()
    with pytest.raises(AuthFailedError):
        print("Sending 4")
        auth_connection.add_auth("digest", "user1")
    zk_auth_failure_workaround()
    zk_stop_and_close(auth_connection)


def test_bad_auth_6(started_cluster):
    auth_connection = get_fake_zk()
    with pytest.raises(AuthFailedError):
        print("Sending 5")
        auth_connection.add_auth("digest", "user1:password:otherpassword")
    zk_auth_failure_workaround()
    zk_stop_and_close(auth_connection)


def test_bad_auth_7(started_cluster):
    auth_connection = get_fake_zk()
    with pytest.raises(AuthFailedError):
        print("Sending 6")
        auth_connection.add_auth("auth", "user1:password")
    zk_auth_failure_workaround()
    zk_stop_and_close(auth_connection)


def test_bad_auth_8(started_cluster):
    auth_connection = get_fake_zk()
    with pytest.raises(AuthFailedError):
        print("Sending 7")
        auth_connection.add_auth("world", "somebody")
    zk_auth_failure_workaround()
    zk_stop_and_close(auth_connection)


def test_bad_auth_9(started_cluster):
    auth_connection = get_fake_zk()
    with pytest.raises(InvalidACLError):
        print("Sending 8")
        auth_connection.create(
            "/test_bad_acl",
            b"data",
            acl=[
                make_acl(
                    "dasd",
                    "",
                    read=True,
                    write=False,
                    create=True,
                    delete=True,
                    admin=True,
                )
            ],
            ephemeral=True,
        )
    zk_auth_failure_workaround()
    zk_stop_and_close(auth_connection)


def test_bad_auth_10(started_cluster):
    auth_connection = get_fake_zk()
    with pytest.raises(InvalidACLError):
        print("Sending 9")
        auth_connection.create(
            "/test_bad_acl",
            b"data",
            acl=[
                make_acl(
                    "digest",
                    "",
                    read=True,
                    write=False,
                    create=True,
                    delete=True,
                    admin=True,
                )
            ],
            ephemeral=True,
        )
    zk_auth_failure_workaround()
    zk_stop_and_close(auth_connection)


def test_bad_auth_11(started_cluster):
    auth_connection = get_fake_zk()
    with pytest.raises(InvalidACLError):
        print("Sending 10")
        auth_connection.create(
            "/test_bad_acl",
            b"data",
            acl=[
                make_acl(
                    "", "", read=True, write=False, create=True, delete=True, admin=True
                )
            ],
            ephemeral=True,
        )
    zk_auth_failure_workaround()
    zk_stop_and_close(auth_connection)


def test_bad_auth_12(started_cluster):
    auth_connection = get_fake_zk()
    with pytest.raises(InvalidACLError):
        print("Sending 11")
        auth_connection.create(
            "/test_bad_acl",
            b"data",
            acl=[
                make_acl(
                    "digest",
                    "dsdasda",
                    read=True,
                    write=False,
                    create=True,
                    delete=True,
                    admin=True,
                )
            ],
            ephemeral=True,
        )
    zk_auth_failure_workaround()
    zk_stop_and_close(auth_connection)


def test_bad_auth_13(started_cluster):
    auth_connection = get_fake_zk()
    with pytest.raises(InvalidACLError):
        print("Sending 12")
        auth_connection.create(
            "/test_bad_acl",
            b"data",
            acl=[
                make_acl(
                    "digest",
                    "dsad:DSAa:d",
                    read=True,
                    write=False,
                    create=True,
                    delete=True,
                    admin=True,
                )
            ],
            ephemeral=True,
        )
    zk_auth_failure_workaround()
    zk_stop_and_close(auth_connection)


def test_world_anyone_specific_permissions(started_cluster):
    """Test that world:anyone ACLs support specific permissions instead of granting all permissions"""
    connection = None
    no_auth_connection = None

    try:
        connection = get_fake_zk()
        connection.add_auth("digest", "user1:password1")

        # Create node with world:anyone read-only permission
        connection.create(
            "/test_world_anyone_read_only",
            b"data",
            acl=[
                make_acl(
                    "world",
                    "anyone",
                    read=True,
                    write=False,
                    create=False,
                    delete=False,
                    admin=False,
                ),
                make_acl("auth", "", all=True),
            ],
        )

        # Create node with world:anyone write-only permission
        connection.create(
            "/test_world_anyone_write_only",
            b"data",
            acl=[
                make_acl(
                    "world",
                    "anyone",
                    read=False,
                    write=True,
                    create=False,
                    delete=False,
                    admin=False,
                ),
                make_acl("auth", "", all=True),
            ],
        )

        # Create node with world:anyone create-only permission
        connection.create(
            "/test_world_anyone_create_only",
            b"data",
            acl=[
                make_acl(
                    "world",
                    "anyone",
                    read=False,
                    write=False,
                    create=True,
                    delete=False,
                    admin=False,
                ),
                make_acl("auth", "", all=True),
            ],
        )

        # Create node with world:anyone delete-only permission and auth all permission
        connection.create(
            "/test_world_anyone_delete_only",
            b"data",
            acl=[
                make_acl(
                    "world",
                    "anyone",
                    read=False,
                    write=False,
                    create=False,
                    delete=True,
                    admin=False,
                ),
                make_acl("auth", "", all=True),
            ],
        )

        # Create child node for testing delete permission on parent
        connection.create(
            "/test_world_anyone_delete_only/child",
            b"child_data",
            acl=[
                make_acl(
                    "world",
                    "anyone",
                    read=False,
                    write=False,
                    create=False,
                    delete=True,
                    admin=False,
                ),
                make_acl("auth", "", all=True),
            ],
        )

        # Create node with world:anyone admin-only permission
        connection.create(
            "/test_world_anyone_admin_only",
            b"data",
            acl=[
                make_acl(
                    "world",
                    "anyone",
                    read=False,
                    write=False,
                    create=False,
                    delete=False,
                    admin=True,
                ),
                make_acl("auth", "", all=True),
            ],
        )

        # Test with a new unauthenticated connection
        no_auth_connection = get_fake_zk()

        # Test read-only permissions
        assert no_auth_connection.get("/test_world_anyone_read_only")[0] == b"data"
        with pytest.raises(NoAuthError):
            no_auth_connection.set("/test_world_anyone_read_only", b"new_data")

        # Test write-only permissions
        with pytest.raises(NoAuthError):
            no_auth_connection.get("/test_world_anyone_write_only")
        no_auth_connection.set("/test_world_anyone_write_only", b"new_data")

        # Test create-only permissions
        with pytest.raises(NoAuthError):
            no_auth_connection.get("/test_world_anyone_create_only")
        with pytest.raises(NoAuthError):
            no_auth_connection.set("/test_world_anyone_create_only", b"new_data")
        # Create operation should work
        no_auth_connection.create("/test_world_anyone_create_only/new_child", b"child")

        # Test delete-only permissions
        with pytest.raises(NoAuthError):
            no_auth_connection.get("/test_world_anyone_delete_only")
        with pytest.raises(NoAuthError):
            no_auth_connection.set("/test_world_anyone_delete_only", b"new_data")
        # Delete operation should work
        no_auth_connection.delete("/test_world_anyone_delete_only/child")

        # Test admin-only permissions
        with pytest.raises(NoAuthError):
            no_auth_connection.get("/test_world_anyone_admin_only")
        with pytest.raises(NoAuthError):
            no_auth_connection.set("/test_world_anyone_admin_only", b"new_data")
        # Admin operations (like get_acls, set_acls) should work
        acls, stat = no_auth_connection.get_acls("/test_world_anyone_admin_only")
        assert len(acls) == 2
        assert acls[0].id.scheme == "world"
        assert acls[0].id.id == "anyone"
        assert acls[0].perms == 16  # Admin permission only

        # Test combined permissions
        connection.create(
            "/test_world_anyone_read_write",
            b"data",
            acl=[
                make_acl(
                    "world",
                    "anyone",
                    read=True,
                    write=True,
                    create=False,
                    delete=False,
                    admin=False,
                ),
                make_acl("auth", "", all=True),
            ],
        )

        # Should be able to read and write, but not create or delete
        assert no_auth_connection.get("/test_world_anyone_read_write")[0] == b"data"
        no_auth_connection.set("/test_world_anyone_read_write", b"updated_data")
        assert (
            no_auth_connection.get("/test_world_anyone_read_write")[0]
            == b"updated_data"
        )

        # Cleanup created nodes
        connection.delete("/test_world_anyone_create_only/new_child")
        connection.delete("/test_world_anyone_create_only")
        connection.delete("/test_world_anyone_delete_only")
        connection.delete("/test_world_anyone_read_only")
        connection.delete("/test_world_anyone_write_only")
        connection.delete("/test_world_anyone_admin_only")
        connection.delete("/test_world_anyone_read_write")

    finally:
        zk_stop_and_close(connection)
        zk_stop_and_close(no_auth_connection)


def test_auth_snapshot(started_cluster):
    connection = None
    connection1 = None
    connection2 = None

    try:
        connection = get_fake_zk()
        connection.add_auth("digest", "user1:password1")

        connection.create(
            "/test_snapshot_acl",
            b"data",
            acl=[make_acl("auth", "", all=True)],
        )

        connection1 = get_fake_zk()
        connection1.add_auth("digest", "user2:password2")

        connection1.create(
            "/test_snapshot_acl1",
            b"data",
            acl=[make_acl("auth", "", all=True)],
        )

        connection2 = get_fake_zk()

        connection2.create(
            "/test_snapshot_acl2",
            b"data",
        )

        for i in range(100):
            connection.create(
                f"/test_snapshot_acl/path{i}",
                b"data",
                acl=[make_acl("auth", "", all=True)],
            )

        node.restart_clickhouse()

        zk_stop_and_close(connection)
        connection = get_fake_zk()

        with pytest.raises(NoAuthError):
            connection.get("/test_snapshot_acl")

        connection.add_auth("digest", "user1:password1")

        assert connection.get("/test_snapshot_acl")[0] == b"data"

        with pytest.raises(NoAuthError):
            connection.get("/test_snapshot_acl1")

        assert connection.get("/test_snapshot_acl2")[0] == b"data"

        for i in range(100):
            assert connection.get(f"/test_snapshot_acl/path{i}")[0] == b"data"

        zk_stop_and_close(connection1)
        connection1 = get_fake_zk()
        connection1.add_auth("digest", "user2:password2")

        assert connection1.get("/test_snapshot_acl1")[0] == b"data"

        with pytest.raises(NoAuthError):
            connection1.get("/test_snapshot_acl")

        zk_stop_and_close(connection2)
        connection2 = get_fake_zk()
        assert connection2.get("/test_snapshot_acl2")[0] == b"data"
        with pytest.raises(NoAuthError):
            connection2.get("/test_snapshot_acl")

        with pytest.raises(NoAuthError):
            connection2.get("/test_snapshot_acl1")
    finally:
        for i in range(100):
            connection.delete(f"/test_snapshot_acl/path{i}")
        connection.delete("/test_snapshot_acl")
        connection.delete("/test_snapshot_acl1")
        connection.delete("/test_snapshot_acl2")
        zk_stop_and_close(connection)
        zk_stop_and_close(connection1)
        zk_stop_and_close(connection2)


@pytest.mark.parametrize(("get_zk"), [get_genuine_zk, get_fake_zk])
def test_get_set_acl(started_cluster, get_zk):
    auth_connection = None
    other_auth_connection = None
    try:
        auth_connection = get_zk()
        auth_connection.add_auth("digest", "username1:secret1")
        auth_connection.add_auth("digest", "username2:secret2")

        auth_connection.create(
            "/test_set_get_acl",
            b"data",
            acl=[make_acl("auth", "", all=True)],
            ephemeral=True,
        )

        acls, stat = auth_connection.get_acls("/test_set_get_acl")

        assert stat.aversion == 0
        assert len(acls) == 2
        for acl in acls:
            assert acl.acl_list == ["ALL"]
            assert acl.id.scheme == "digest"
            assert acl.perms == 31
            assert acl.id.id in (
                "username1:eGncMdBgOfGS/TCojt51xWsWv/Y=",
                "username2:qgSSumukVlhftkVycylbHNvxhFU=",
            )

        other_auth_connection = get_zk()
        other_auth_connection.add_auth("digest", "username1:secret1")
        other_auth_connection.add_auth("digest", "username3:secret3")
        other_auth_connection.set_acls(
            "/test_set_get_acl",
            acls=[
                make_acl(
                    "auth",
                    "",
                    read=True,
                    write=False,
                    create=True,
                    delete=True,
                    admin=True,
                )
            ],
        )

        acls, stat = other_auth_connection.get_acls("/test_set_get_acl")

        assert stat.aversion == 1
        assert len(acls) == 2
        for acl in acls:
            assert acl.acl_list == ["READ", "CREATE", "DELETE", "ADMIN"]
            assert acl.id.scheme == "digest"
            assert acl.perms == 29
            assert acl.id.id in (
                "username1:eGncMdBgOfGS/TCojt51xWsWv/Y=",
                "username3:CvWITOxxTwk+u6S5PoGlQ4hNoWI=",
            )

        with pytest.raises(KazooException):
            other_auth_connection.set_acls(
                "/test_set_get_acl", acls=[make_acl("auth", "", all=True)], version=0
            )
    finally:
        zk_stop_and_close(auth_connection)
        zk_stop_and_close(other_auth_connection)
