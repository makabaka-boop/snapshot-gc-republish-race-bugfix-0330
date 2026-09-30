"""两阶段垃圾回收：标记候选、复核删除、租约/快照保活、竞争与幂等。"""

from __future__ import annotations

import threading
from datetime import timedelta

import pytest

from objstore import (
    DuplicateGCRunError,
    ObjectNotFoundError,
    Repository,
    UnknownGCRunError,
)

from conftest import ScriptedHooks, assert_consistent


def _populate(repo):
    kept = repo.put(b"snapshot-kept")
    leased = repo.put(b"lease-kept")
    garbage = repo.put(b"garbage")
    sid = repo.publish_snapshot([kept])
    lid = repo.grant_lease([leased], ttl=timedelta(hours=1))
    return kept, leased, garbage, sid, lid


def test_mark_only_marks_and_does_not_delete(repo, clock):
    kept, leased, garbage, sid, lid = _populate(repo)
    mark = repo.gc_mark()
    assert mark.candidate_ids == [garbage]
    # 第一阶段结束后：候选仍在数据库，文件仍在磁盘。
    assert repo.exists(garbage)
    assert repo.get(garbage) == b"garbage"
    assert_consistent(repo)


def test_sweep_deletes_only_unreferenced(repo, clock):
    kept, leased, garbage, sid, lid = _populate(repo)
    mark = repo.gc_mark()
    result = repo.gc_sweep(mark.run_id)
    assert result.reclaimed == [garbage]
    assert result.rescued == []
    assert not repo.exists(garbage)
    assert repo.exists(kept) and repo.exists(leased)
    assert repo.get(kept) == b"snapshot-kept"
    assert repo.get(leased) == b"lease-kept"
    assert_consistent(repo)


def test_expired_lease_does_not_protect(repo, clock):
    oid = repo.put(b"temp-export")
    repo.grant_lease([oid], ttl=timedelta(minutes=30))
    clock.advance(timedelta(minutes=31))

    result = repo.gc()
    assert result.reclaimed == [oid]
    assert not repo.exists(oid)
    assert repo.list_leases() == []
    assert_consistent(repo)


def test_lease_valid_at_mark_is_protected_for_the_whole_run(repo, clock):
    """标记时未到期的租约在整个回收运行内都保活，即使窗口内到期也不删。

    到期租约的清理属于下一次回收：保证 GC 绝不删除“标记时仍被根引用”的对象。
    """

    oid = repo.put(b"will-expire")
    repo.grant_lease([oid], ttl=timedelta(minutes=10))

    mark = repo.gc_mark()
    assert mark.candidate_ids == []  # 标记时租约未到期，对象存活

    clock.advance(timedelta(minutes=11))  # 复核时租约已到期
    result = repo.gc_sweep(mark.run_id)
    assert result.reclaimed == []
    assert repo.exists(oid)
    assert_consistent(repo)

    # 下一次回收以新的“当前”重新标记：到期租约不再保活，对象被删除。
    result2 = repo.gc()
    assert result2.reclaimed == [oid]
    assert not repo.exists(oid)
    assert_consistent(repo)


def test_snapshot_published_between_phases_rescues_candidate(repo, clock):
    """关键竞争：标记与复核之间发布的新快照必须救回候选对象。"""

    oid = repo.put(b"rescued-by-race")
    mark = repo.gc_mark()
    assert mark.candidate_ids == [oid]

    rescued_sid = repo.publish_snapshot([oid])  # 两阶段之间成为新根
    assert_consistent(repo)

    result = repo.gc_sweep(mark.run_id)
    assert result.rescued == [oid]
    assert result.reclaimed == []
    assert repo.exists(oid)
    assert repo.get(oid) == b"rescued-by-race"
    assert repo.get_snapshot(rescued_sid).object_ids == [oid]
    assert_consistent(repo)

    # 删除快照后重新回收：候选这次确实消失。
    repo.delete_snapshot(rescued_sid)
    result2 = repo.gc()
    assert result2.reclaimed == [oid]
    assert not repo.exists(oid)
    assert_consistent(repo)


def test_lease_granted_between_phases_rescues_candidate(repo, clock):
    oid = repo.put(b"rescued-by-lease")
    mark = repo.gc_mark()
    repo.grant_lease([oid], ttl=timedelta(hours=2))

    result = repo.gc_sweep(mark.run_id)
    assert result.rescued == [oid]
    assert repo.exists(oid)
    assert_consistent(repo)


def test_concurrent_publish_inside_sweep_window_has_deterministic_outcome(
    repo_root, clock
):
    """发布与“复核→删文件”窗口真正并发：谁先拿到 IMMEDIATE 锁谁赢。

    发布线程在 ``before_reclaim`` 钩子里与回收线程在屏障处会合：
    此时复核已提交、删除事务已打开。发布必须阻塞到删除提交，随后确定性地
    收到 ObjectNotFoundError；数据库里绝不允许出现指向已删除文件的快照引用。
    """

    r_main = Repository(repo_root, clock=clock)
    oid = r_main.put(b"concurrent-victim")
    barrier = threading.Barrier(2)
    hooks = ScriptedHooks(
        on_before_reclaim=lambda run_id, object_id: barrier.wait(timeout=10)
    )
    r_gc = Repository(repo_root, clock=clock, hooks=hooks)

    mark = r_gc.gc_mark()
    assert mark.candidate_ids == [oid]

    publish_error: list[Exception] = []
    publish_done = threading.Event()

    def publish() -> None:
        barrier.wait(timeout=10)  # 与 before_reclaim 同一时刻会合
        r_pub = Repository(repo_root, clock=clock)
        try:
            r_pub.publish_snapshot([oid])
        except ObjectNotFoundError as exc:
            publish_error.append(exc)
        except Exception as exc:  # noqa: BLE001 - 任何其他错误都使测试失败
            publish_error.append(exc)
        finally:
            publish_done.set()

    t = threading.Thread(target=publish)
    t.start()

    result = r_gc.gc_sweep(mark.run_id)
    publish_done.wait(timeout=10)
    t.join(timeout=10)

    assert not t.is_alive()
    assert result.reclaimed == [oid]
    assert len(publish_error) == 1
    assert isinstance(publish_error[0], ObjectNotFoundError)
    assert r_main.list_snapshots() == []
    assert not r_main.exists(oid)
    assert_consistent(r_main)


def test_concurrent_publish_between_phases_with_barrier(repo_root, clock):
    """在 sweep_begin 阶段屏障处并发发布：发布先于任何复核完成，对象被救回。"""

    r_main = Repository(repo_root, clock=clock)
    oid = r_main.put(b"concurrent-rescue")

    sweep_arrived = threading.Event()
    publish_done = threading.Event()

    def on_sweep_begin(run_id, candidates):
        # 阶段屏障：通知发布线程“两阶段之间”窗口已开启，并阻塞到发布完成。
        sweep_arrived.set()
        assert publish_done.wait(timeout=10)

    hooks = ScriptedHooks(on_sweep_begin=on_sweep_begin)
    r_gc = Repository(repo_root, clock=clock, hooks=hooks)

    mark = r_gc.gc_mark()

    def publish() -> None:
        assert sweep_arrived.wait(timeout=10)
        Repository(repo_root, clock=clock).publish_snapshot([oid])
        publish_done.set()

    t = threading.Thread(target=publish)
    t.start()

    result = r_gc.gc_sweep(mark.run_id)
    t.join(timeout=10)

    assert not t.is_alive()
    assert result.rescued == [oid]
    assert r_main.exists(oid)
    assert_consistent(r_main)


def test_sweep_is_idempotent_and_repeat_gc_runs_are_stable(repo, clock):
    oids = [repo.put(f"obj-{i}".encode()) for i in range(3)]
    sid = repo.publish_snapshot([oids[0]])

    first = repo.gc()
    assert first.reclaimed == sorted([oids[1], oids[2]])

    # 对同一个 run 重复复核：确定性空操作。
    again = repo.gc_sweep(first.run_id)
    assert again.reclaimed == sorted([oids[1], oids[2]])
    assert again.rescued == []

    # 再次发起完整回收：没有候选，已有对象保持存活。
    third = repo.gc()
    assert third.reclaimed == []
    assert repo.exists(oids[0])
    assert not repo.exists(oids[1])
    assert_consistent(repo)


def test_reput_and_publish_inside_delete_window_rescues_object(repo_root, clock):
    """核心竞争：元数据删除已提交、文件未删的窗口内重写同一内容并发布。

    重新发布必须成功，且旧回收不得再删除该文件：put 在插入对象行的同一
    事务里把 reclaimed 候选改判 rescued，旧 run 的文件删除阶段在写锁内
    复核到复活后放弃 unlink。旧 run 重试也不再报一致性错误。
    """

    r_main = Repository(repo_root, clock=clock)
    oid = r_main.put(b"resurrect-me")

    window_open = threading.Event()
    resume = threading.Event()

    def on_reclaim_committed(run_id, object_id):
        # 删除窗口：对象行已删、文件仍在。阻塞到客户端完成重写+发布。
        window_open.set()
        assert resume.wait(timeout=10)

    hooks = ScriptedHooks(on_reclaim_committed=on_reclaim_committed)
    r_gc = Repository(repo_root, clock=clock, hooks=hooks)
    mark = r_gc.gc_mark()
    assert mark.candidate_ids == [oid]

    result_box: dict = {}

    def sweep() -> None:
        result_box["result"] = r_gc.gc_sweep(mark.run_id)

    t = threading.Thread(target=sweep)
    t.start()
    assert window_open.wait(timeout=10)

    # 窗口内：同一内容重新写入（文件还在，put 直接沿用）并立即发布。
    r_client = Repository(repo_root, clock=clock)
    assert r_client.put(b"resurrect-me") == oid
    sid = r_client.publish_snapshot([oid])

    resume.set()
    t.join(timeout=10)
    assert not t.is_alive()

    result = result_box["result"]
    assert result.reclaimed == []
    assert result.rescued == [oid]
    assert r_main.get(oid) == b"resurrect-me"
    assert r_main.get_snapshot(sid).object_ids == [oid]
    assert_consistent(r_main)

    # 旧 run 幂等重试：复活是合法结果，不报一致性错误。
    again = r_gc.gc_sweep(mark.run_id)
    assert again.reclaimed == []
    assert again.rescued == [oid]
    assert_consistent(r_main)


def test_reput_after_completed_reclaim_is_legal_resurrection(repo, clock):
    """回收彻底完成后重写同一内容：合法复活，一致性核对与旧 run 重试都不报错。"""

    oid = repo.put(b"comeback")
    result = repo.gc()
    assert result.reclaimed == [oid]
    assert not repo.exists(oid)

    assert repo.put(b"comeback") == oid
    assert repo.get(oid) == b"comeback"
    assert_consistent(repo)  # 不得报 “reclaimed 候选仍有对象行”

    retry = repo.gc_sweep(result.run_id)
    assert retry.reclaimed == []
    assert retry.rescued == [oid]
    assert_consistent(repo)

    # 复活对象被新快照引用后，下一轮回收同样不会动它。
    sid = repo.publish_snapshot([oid])
    assert repo.gc().reclaimed == []
    assert repo.get_snapshot(sid).object_ids == [oid]
    assert_consistent(repo)


def test_resurrected_but_still_unreferenced_is_reclaimed_by_next_run(repo, clock):
    """复活后仍无根引用：下一轮回收正常删除（复活不等于永久保活）。"""

    oid = repo.put(b"ephemeral")
    first = repo.gc()
    assert first.reclaimed == [oid]

    assert repo.put(b"ephemeral") == oid  # 复活但无引用
    assert_consistent(repo)

    second = repo.gc()
    assert second.reclaimed == [oid]
    assert not repo.exists(oid)
    assert_consistent(repo)


def test_unknown_and_duplicate_run_ids(repo, clock):
    with pytest.raises(UnknownGCRunError):
        repo.gc_sweep("deadbeef")
    mark = repo.gc_mark(run_id="fixed-run")
    with pytest.raises(DuplicateGCRunError):
        repo.gc_mark(run_id="fixed-run")
    assert mark.run_id == "fixed-run"
    repo.gc_sweep("fixed-run")
    assert_consistent(repo)


def test_unreferenced_objects_added_after_mark_are_not_touched_by_old_run(repo, clock):
    oid = repo.put(b"early-garbage")
    mark = repo.gc_mark()
    late = repo.put(b"late-garbage")  # 标记后才出现

    result = repo.gc_sweep(mark.run_id)
    assert result.reclaimed == [oid]
    assert repo.exists(late)  # 旧 run 不负责新对象

    result2 = repo.gc()
    assert result2.reclaimed == [late]
    assert not repo.exists(late)
    assert_consistent(repo)
