"""数据库与对象目录之间的一致性检查（测试在每一步后调用）。"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass, field
from pathlib import Path

from .repo import Repository

# 化身文件名：<id 后 62 位>-<32 位 hex nonce>，分片目录名是 id 前 2 位。
_SUFFIX_LEN = 1 + 32  # "-" + nonce


@dataclass
class ConsistencyReport:
    """一致性核对结果。``violations`` 为空即一致。"""

    violations: list[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not self.violations

    def raise_if_bad(self) -> None:
        if self.violations:
            raise AssertionError("仓库一致性被破坏:\n  - " + "\n  - ".join(self.violations))


def check_consistency(repo: Repository) -> ConsistencyReport:
    """核对元数据与对象目录的对应关系及引用完整性。

    每个 ``objects`` 行都必须在磁盘上有它记录的那个**物理化身**文件。反过来，
    磁盘上的化身文件若不被任何行引用，只有在它是某次回收“元数据已提交、文件
    尚未 unlink”的待清理旧化身（由 reclaimed 候选跟踪）时才合法，
    其余都算孤儿。正常 API 操作、两阶段回收的每个边界，以及崩溃后重试完成之后，
    本检查都应当返回空报告。
    """

    violations: list[str] = []
    conn = repo._connect()
    try:
        # oid -> 当前行使用的化身 nonce。
        live_blobs: dict[str, str] = {
            r[0]: r[1] for r in conn.execute("SELECT id, blob FROM objects").fetchall()
        }

        # 外键约束本身由 PRAGMA foreign_keys 保证，这里再显式核对引用关系。
        for table, col in ("snapshot_objects", "object_id"), ("lease_objects", "object_id"):
            rows = conn.execute(
                f"SELECT DISTINCT {col} FROM {table} WHERE {col} NOT IN"
                " (SELECT id FROM objects)"
            ).fetchall()
            for r in rows:
                violations.append(f"{table} 引用了不存在的对象: {r[0]}")

        # 回收审计：reclaimed 记录的是标记那一代已让位的旧化身；若该逻辑 id
        # 当前仍有行，行必须指向**另一个**（复活的新）化身，绝不能仍指向已判删
        # 的旧化身（否则旧文件补删后行就会悬空）。
        rows = conn.execute(
            "SELECT gc.object_id, gc.blob FROM gc_candidates gc"
            " WHERE gc.state = 'reclaimed'"
        ).fetchall()
        pending_blobs: set[tuple[str, str]] = set()
        for r in rows:
            oid, old_blob = r[0], r[1]
            pending_blobs.add((oid, old_blob))
            if live_blobs.get(oid) == old_blob:
                violations.append(
                    f"候选 {oid} 的化身 {old_blob} 已判删，但对象行仍指向它"
                )

        # 所有标记完成的回收运行：候选对象要么存活（被救回），要么 reclaimed，
        # 不能停在 candidate。
        rows = conn.execute(
            "SELECT gc.run_id, gc.object_id FROM gc_candidates gc"
            " JOIN gc_runs r ON r.id = gc.run_id"
            " WHERE gc.state = 'candidate' AND r.status = 'swept'"
        ).fetchall()
        for r in rows:
            violations.append(
                f"回收运行 {r['run_id']} 已完成但候选仍未处理: {r['object_id']}"
            )
    finally:
        conn.close()

    # 目录侧：枚举所有物理化身文件，解析出 (oid, nonce)。
    disk_blobs: set[tuple[str, str]] = set()
    for shard in sorted(repo.objects_dir.iterdir()):
        if shard.name.startswith(".") or not shard.is_dir():
            if shard.is_file() and shard.name.startswith(".tmp-"):
                violations.append(f"残留临时文件: {shard}")
            continue
        for f in sorted(shard.iterdir()):
            if f.name.startswith(".tmp-"):
                violations.append(f"残留临时文件: {f}")
                continue
            if not f.is_file():
                violations.append(f"对象目录中存在非文件条目: {f}")
                continue
            stem = f.name
            if len(stem) == 62 + _SUFFIX_LEN and stem[62] == "-":
                oid = shard.name + stem[:62]
                nonce = stem[63:]
            else:
                violations.append(f"对象文件名不符合化身命名 <id>-<nonce>: {f}")
                continue
            if len(oid) != 64:
                violations.append(f"对象文件名无法构成合法 id: {f}")
                continue
            try:
                int(nonce, 16)
            except ValueError:
                violations.append(f"对象化身 nonce 不是十六进制: {f}")
                continue
            disk_blobs.add((oid, nonce))

    expected_blobs = set(live_blobs.items())

    # 行存在但其记录的化身文件缺失 → 数据库指向已删除文件。
    for oid, nonce in sorted(expected_blobs - disk_blobs):
        violations.append(f"数据库引用了已删除/缺失的对象文件: {oid} (blob {nonce})")

    # 磁盘化身既不被任何行引用、也不在待清理集合中 → 孤儿文件。
    for oid, nonce in sorted(disk_blobs - expected_blobs - pending_blobs):
        violations.append(
            f"对象化身文件存在但数据库没有记录（孤儿文件）: {oid} (blob {nonce})"
        )

    return ConsistencyReport(violations=violations)
