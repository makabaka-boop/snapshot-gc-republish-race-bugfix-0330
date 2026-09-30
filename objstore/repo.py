"""仓库核心实现。

不变量（每个已提交状态、以及崩溃恢复后的重试都必须满足）：

1. ``objects`` 中的每一行都对应 ``objects/`` 目录下一个**物理化身文件**
   ``objects/<id前2位>/<id后62位>-<nonce>``，内容哈希等于 id。
2. 快照/租约引用的每个对象都存在（外键 ``ON DELETE RESTRICT`` 强制）。
3. 一个对象只有在“当前没有任何已发布快照或未到期租约引用它”时才会被删除。

物理化身（incarnation）解决“同内容复活”竞争：每次 ``put`` 一个**当前无行**的
对象都生成新的一次性 ``nonce``（新文件名），绝不复用残留的旧文件。一次回收
在“元数据删除已提交、旧文件尚未 unlink”的窗口里，若客户端重新 put 相同内容
并立即发布快照，复活产生的是**新化身**；该旧回收重试时只会 unlink 它标记时
记录的**旧化身**文件，删不到新文件，因此新快照始终有效；重试也不会因同一 id
重新出现而报一致性错误。

垃圾回收分两阶段：

* ``gc_mark``：在单个事务里计算存活集（快照 ∪ 未到期租约），把其余对象
  作为候选写入 ``gc_candidates``，不删除任何文件。
* ``gc_sweep``：对每个候选在一个 **IMMEDIATE 写事务** 里重新复核。复核通过后，
  **先在该事务中删除元数据（objects 行、候选标记为 reclaimed）并提交，随后才
  删除文件**。这样崩溃只会留下“数据库不再引用、但文件尚未删除”的待清理文件
  （由 gc_candidates 记录、重试时补删），绝不会出现“数据库指向已删除文件”，
  也绝不会删掉数据库仍引用的文件。

两阶段之间（或扫描其他候选时）获得新引用的候选转为 ``rescued``；删除窗口内
到达的引用有两种确定结果：未重新写入对象则因行不可见收到对象不存在错误；
重新写入相同内容（复活）则写入新化身并正常成功，旧回收只清理它自己的旧化身。
"""

from __future__ import annotations

import contextlib
import hashlib
import os
import re
import sqlite3
import tempfile
import uuid
from dataclasses import dataclass, field
from datetime import timedelta
from pathlib import Path
from types import TracebackType
from typing import Iterator

from .clocks import Clock, SystemClock, to_epoch_micros
from .exceptions import (
    DuplicateGCRunError,
    InternalConsistencyError,
    InvalidObjectIdError,
    LeaseNotFoundError,
    ObjectNotFoundError,
    SnapshotNotFoundError,
    UnknownGCRunError,
)
from .hooks import GCHooks, NullHooks

_SCHEMA_PATH = Path(__file__).with_name("schema.sql")
_OID_RE = re.compile(r"^[0-9a-f]{64}$")
_BUSY_TIMEOUT_SECONDS = 30.0


def _validate_oid(oid: str) -> str:
    if not isinstance(oid, str) or not _OID_RE.fullmatch(oid):
        raise InvalidObjectIdError(f"非法对象 id: {oid!r}（应为 64 位小写十六进制）")
    return oid


def _normalize_oid_list(object_ids) -> list[str]:
    """校验、去重并排序一组对象 id（快照/租约是集合而非序列）。"""

    if isinstance(object_ids, (str, bytes)):
        raise TypeError("object_ids 必须是字符串 id 的可迭代对象，而不是单个字符串")
    unique = {_validate_oid(oid) for oid in object_ids}
    return sorted(unique)


def _snapshot_content_id(object_ids: list[str]) -> str:
    """快照 id 由其成员集合确定性派生：相同集合得到相同快照。"""

    h = hashlib.sha256()
    for oid in object_ids:
        h.update(b"snap\x00")
        h.update(bytes.fromhex(oid))
        h.update(b"\x00")
    return h.hexdigest()


@dataclass(frozen=True)
class SnapshotInfo:
    id: str
    object_ids: list[str] = field(default_factory=list)
    created_at_epoch_micros: int = 0


@dataclass(frozen=True)
class LeaseInfo:
    id: str
    object_ids: list[str] = field(default_factory=list)
    created_at_epoch_micros: int = 0
    expires_at_epoch_micros: int = 0

    def is_expired(self, now_epoch_micros: int) -> bool:
        return self.expires_at_epoch_micros <= now_epoch_micros


@dataclass(frozen=True)
class GCMarkResult:
    run_id: str
    candidate_ids: list[str]


@dataclass(frozen=True)
class GCSweepResult:
    run_id: str
    reclaimed: list[str]
    rescued: list[str]


class Repository:
    """本地内容寻址对象仓库。

    :param root: 仓库根目录（不存在会自动创建），元数据在 ``root/meta.sqlite3``，
        对象文件在 ``root/objects/xx/yyyy...``。
    :param clock: 时间源，测试可注入可控时钟。
    :param hooks: 回收阶段钩子（屏障/崩溃注入用），不影响正确性。
    """

    def __init__(
        self,
        root: str | os.PathLike[str],
        clock: Clock | None = None,
        hooks: GCHooks | None = None,
    ) -> None:
        self.root = Path(root)
        self.objects_dir = self.root / "objects"
        self.db_path = self.root / "meta.sqlite3"
        self.clock: Clock = clock or SystemClock()
        self.hooks: GCHooks = hooks or NullHooks()
        self.root.mkdir(parents=True, exist_ok=True)
        self.objects_dir.mkdir(parents=True, exist_ok=True)
        self._initialize_db()

    # ------------------------------------------------------------------ 基础

    def _connect(self) -> sqlite3.Connection:
        # isolation_level=None：完全由我们自己控制 BEGIN/COMMIT，
        # 从而保证“复核+删文件+删行”处于同一个 IMMEDIATE 事务。
        conn = sqlite3.connect(
            self.db_path,
            timeout=_BUSY_TIMEOUT_SECONDS,
            isolation_level=None,
        )
        conn.row_factory = sqlite3.Row
        conn.execute(f"PRAGMA busy_timeout = {int(_BUSY_TIMEOUT_SECONDS * 1000)}")
        conn.execute("PRAGMA journal_mode = WAL")
        conn.execute("PRAGMA synchronous = NORMAL")
        conn.execute("PRAGMA foreign_keys = ON")
        return conn

    def _initialize_db(self) -> None:
        conn = self._connect()
        try:
            conn.executescript(_SCHEMA_PATH.read_text(encoding="utf-8"))
            version = conn.execute(
                "SELECT value FROM schema_meta WHERE key = 'version'"
            ).fetchone()
            if version is None or int(version[0]) < 2:
                # v1 布局对象行没有 blob 列，无法做物理化身隔离，需要重建仓库。
                raise InternalConsistencyError(
                    "对象仓库元数据版本过旧（< 2，缺少物理化身列），"
                    "请清空后重新初始化"
                )
        finally:
            conn.close()

    @contextlib.contextmanager
    def _tx(self) -> Iterator[sqlite3.Connection]:
        conn = self._connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            yield conn
            conn.execute("COMMIT")
        except BaseException:
            with contextlib.suppress(Exception):
                conn.execute("ROLLBACK")
            raise
        finally:
            conn.close()

    def _now(self) -> int:
        return to_epoch_micros(self.clock.now())

    def _path_for_blob(self, object_id: str, blob: str) -> Path:
        """物理化身路径：objects/<id前2位>/<id后62位>-<nonce>。"""

        return self.objects_dir / object_id[:2] / f"{object_id[2:]}-{blob}"

    def _allocate_blob(self, conn: sqlite3.Connection, object_id: str) -> tuple[str, Path]:
        """在写事务内挑一个全局未占用的化身 nonce，返回 (nonce, 目标路径)。

        nonce 一次性使用：复活得到的总是新文件名，从不在物理路径上与待清理的
        旧文件相撞。冲突（同 nonce 已被任何现存行占用）时换一个重试。
        """

        for _ in range(8):
            nonce = uuid.uuid4().hex
            taken = conn.execute(
                "SELECT 1 FROM objects WHERE blob = ?", (nonce,)
            ).fetchone()
            path = self._path_for_blob(object_id, nonce)
            if taken is None and not path.exists():
                return nonce, path
        raise InternalConsistencyError("无法为对象分配唯一的物理化身名")

    def _blob_map(self, conn: sqlite3.Connection, oids: list[str]) -> dict[str, str]:
        if not oids:
            return {}
        placeholders = ",".join("?" * len(oids))
        return {
            r[0]: r[1]
            for r in conn.execute(
                f"SELECT id, blob FROM objects WHERE id IN ({placeholders})", oids
            ).fetchall()
        }

    @staticmethod
    def _fsync_dir(path: Path) -> None:
        try:
            fd = os.open(path, os.O_RDONLY)
        except OSError:
            return
        try:
            os.fsync(fd)
        except OSError:
            pass
        finally:
            os.close(fd)

    def _write_blob_file(self, dest: Path, data: bytes) -> None:
        path = dest
        path.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp_name = tempfile.mkstemp(prefix=".tmp-", dir=path.parent)
        try:
            with os.fdopen(fd, "wb") as f:
                f.write(data)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp_name, path)
            self._fsync_dir(path.parent)
        except BaseException:
            with contextlib.suppress(FileNotFoundError):
                os.unlink(tmp_name)
            raise

    # ------------------------------------------------------------- 对象操作

    def put(self, data: bytes) -> str:
        """写入一个对象，返回其内容 id（SHA-256）。重复写入是幂等的。

        若逻辑 id 当前已有行，直接复用其化身（内容寻址保证内容相同）。
        若没有行（包括“刚被回收、旧文件尚未清理”的窗口），一律分配并写入一个
        **全新的物理化身**，而不是复用目录里的残留文件——这样旧回收的延迟
        unlink 永远删不到这次复活所依赖的文件。
        """

        if not isinstance(data, (bytes, bytearray)):
            raise TypeError("data 必须是 bytes")
        data = bytes(data)
        object_id = hashlib.sha256(data).hexdigest()
        now = self._now()
        with self._tx() as conn:
            exists = conn.execute(
                "SELECT 1 FROM objects WHERE id = ?", (object_id,)
            ).fetchone()
            if exists is not None:
                return object_id
            # 复活（或首次写入）：新 nonce、新文件名，与任何待清理旧文件隔离。
            nonce, dest = self._allocate_blob(conn, object_id)
            self._write_blob_file(dest, data)
            conn.execute(
                "INSERT INTO objects(id, blob, size, created_at) VALUES (?, ?, ?, ?)",
                (object_id, nonce, len(data), now),
            )
        return object_id

    def get(self, object_id: str) -> bytes:
        """读取对象内容；对象不存在抛 :class:`ObjectNotFoundError`。"""

        _validate_oid(object_id)
        conn = self._connect()
        try:
            row = conn.execute(
                "SELECT size, blob FROM objects WHERE id = ?", (object_id,)
            ).fetchone()
        finally:
            conn.close()
        if row is None:
            raise ObjectNotFoundError(object_id)
        path = self._path_for_blob(object_id, row["blob"])
        try:
            data = path.read_bytes()
        except FileNotFoundError:
            raise InternalConsistencyError(
                f"数据库引用了缺失的对象文件: {object_id} (blob {row['blob']})"
            ) from None
        if len(data) != row["size"] or hashlib.sha256(data).hexdigest() != object_id:
            raise InternalConsistencyError(f"对象文件损坏: {object_id}")
        return data

    def exists(self, object_id: str) -> bool:
        _validate_oid(object_id)
        conn = self._connect()
        try:
            row = conn.execute(
                "SELECT 1 FROM objects WHERE id = ?", (object_id,)
            ).fetchone()
        finally:
            conn.close()
        return row is not None

    def list_objects(self) -> list[str]:
        conn = self._connect()
        try:
            return [
                r[0]
                for r in conn.execute("SELECT id FROM objects ORDER BY id").fetchall()
            ]
        finally:
            conn.close()

    # ------------------------------------------------------------- 快照操作

    def publish_snapshot(self, object_ids, *, snapshot_id: str | None = None) -> str:
        """发布不可变快照，使其引用的对象成为存活根。

        相同成员集合得到相同的快照 id，重复发布幂等。引用不存在的对象会抛
        :class:`ObjectNotFoundError`，且不写入任何内容。
        """

        oids = _normalize_oid_list(object_ids)
        sid = snapshot_id or _snapshot_content_id(oids)
        now = self._now()
        with self._tx() as conn:
            row = conn.execute(
                "SELECT 1 FROM snapshots WHERE id = ?", (sid,)
            ).fetchone()
            if row is not None:
                return sid
            missing = self._missing_object_ids(conn, oids)
            if missing:
                raise ObjectNotFoundError(object_ids=missing)
            blobs = self._blob_map(conn, oids)
            for oid in oids:
                if not self._path_for_blob(oid, blobs[oid]).exists():
                    # 行存在但文件缺失属于损坏；添加根引用必须失败而不是制造悬空引用。
                    raise InternalConsistencyError(
                        f"数据库引用了缺失的对象文件: {oid}"
                    )
            conn.execute(
                "INSERT INTO snapshots(id, created_at) VALUES (?, ?)", (sid, now)
            )
            conn.executemany(
                "INSERT INTO snapshot_objects(snapshot_id, object_id) VALUES (?, ?)",
                [(sid, oid) for oid in oids],
            )
        return sid

    def delete_snapshot(self, snapshot_id: str) -> None:
        """删除快照（仅撤销根引用，不删除任何对象）。

        快照不存在时抛 :class:`SnapshotNotFoundError`，结果确定。
        """

        with self._tx() as conn:
            cur = conn.execute(
                "DELETE FROM snapshots WHERE id = ?", (snapshot_id,)
            )
            if cur.rowcount == 0:
                raise SnapshotNotFoundError(f"快照不存在: {snapshot_id}")

    def get_snapshot(self, snapshot_id: str) -> SnapshotInfo:
        conn = self._connect()
        try:
            row = conn.execute(
                "SELECT id, created_at FROM snapshots WHERE id = ?", (snapshot_id,)
            ).fetchone()
            if row is None:
                raise SnapshotNotFoundError(f"快照不存在: {snapshot_id}")
            members = [
                r[0]
                for r in conn.execute(
                    "SELECT object_id FROM snapshot_objects WHERE snapshot_id = ?"
                    " ORDER BY object_id",
                    (snapshot_id,),
                ).fetchall()
            ]
        finally:
            conn.close()
        return SnapshotInfo(
            id=row["id"],
            object_ids=members,
            created_at_epoch_micros=row["created_at"],
        )

    def list_snapshots(self) -> list[str]:
        conn = self._connect()
        try:
            return [
                r[0]
                for r in conn.execute("SELECT id FROM snapshots ORDER BY id").fetchall()
            ]
        finally:
            conn.close()

    # ------------------------------------------------------------- 租约操作

    def grant_lease(
        self,
        object_ids,
        *,
        ttl: timedelta | None = None,
        lease_id: str | None = None,
    ) -> str:
        """为一组对象授予带到期时间的导出租约；未到期租约保住其对象。

        ``ttl`` 默认为一小时。租约在 ``now + ttl`` 时刻到期（到期时刻本身不再
        保活）。引用不存在的对象抛 :class:`ObjectNotFoundError`，不写入任何内容。
        """

        oids = _normalize_oid_list(object_ids)
        ttl = timedelta(hours=1) if ttl is None else ttl
        lid = lease_id or uuid.uuid4().hex
        now = self._now()
        expires_at = now + int(ttl.total_seconds() * 1_000_000)
        with self._tx() as conn:
            dup = conn.execute(
                "SELECT 1 FROM leases WHERE id = ?", (lid,)
            ).fetchone()
            if dup is not None:
                raise DuplicateGCRunError(f"租约 id 已存在: {lid}")
            missing = self._missing_object_ids(conn, oids)
            if missing:
                raise ObjectNotFoundError(object_ids=missing)
            blobs = self._blob_map(conn, oids)
            for oid in oids:
                if not self._path_for_blob(oid, blobs[oid]).exists():
                    raise InternalConsistencyError(
                        f"数据库引用了缺失的对象文件: {oid}"
                    )
            conn.execute(
                "INSERT INTO leases(id, created_at, expires_at) VALUES (?, ?, ?)",
                (lid, now, expires_at),
            )
            conn.executemany(
                "INSERT INTO lease_objects(lease_id, object_id) VALUES (?, ?)",
                [(lid, oid) for oid in oids],
            )
        return lid

    def revoke_lease(self, lease_id: str) -> bool:
        """提前撤销租约。返回是否真的撤销了一个存在的租约（重复撤销返回 False）。"""

        with self._tx() as conn:
            cur = conn.execute("DELETE FROM leases WHERE id = ?", (lease_id,))
            return cur.rowcount > 0

    def get_lease(self, lease_id: str) -> LeaseInfo:
        conn = self._connect()
        try:
            row = conn.execute(
                "SELECT id, created_at, expires_at FROM leases WHERE id = ?",
                (lease_id,),
            ).fetchone()
            if row is None:
                raise LeaseNotFoundError(f"租约不存在: {lease_id}")
            members = [
                r[0]
                for r in conn.execute(
                    "SELECT object_id FROM lease_objects WHERE lease_id = ?"
                    " ORDER BY object_id",
                    (lease_id,),
                ).fetchall()
            ]
        finally:
            conn.close()
        return LeaseInfo(
            id=row["id"],
            object_ids=members,
            created_at_epoch_micros=row["created_at"],
            expires_at_epoch_micros=row["expires_at"],
        )

    def list_leases(self, *, include_expired: bool = True) -> list[str]:
        sql = "SELECT id FROM leases"
        params: tuple = ()
        if not include_expired:
            sql += " WHERE expires_at > ?"
            params = (self._now(),)
        sql += " ORDER BY id"
        conn = self._connect()
        try:
            return [r[0] for r in conn.execute(sql, params).fetchall()]
        finally:
            conn.close()

    # --------------------------------------------------------------- 垃圾回收

    def gc_mark(self, *, run_id: str | None = None) -> GCMarkResult:
        """第一阶段：标记候选。只在数据库中记录候选，不触碰任何对象文件。

        标记开始前会先清理已到期租约（与标记在同一事务中，时钟即时取值）。
        """

        run_id = run_id or uuid.uuid4().hex
        self.hooks.mark_begin(run_id)
        now = self._now()
        with self._tx() as conn:
            dup = conn.execute(
                "SELECT 1 FROM gc_runs WHERE id = ?", (run_id,)
            ).fetchone()
            if dup is not None:
                raise DuplicateGCRunError(f"回收运行 id 已存在: {run_id}")
            self._prune_expired_leases(conn, now)
            all_blobs = {
                r[0]: r[1] for r in conn.execute("SELECT id, blob FROM objects").fetchall()
            }
            live = self._live_object_ids(conn, now)
            candidates = sorted(set(all_blobs) - live)
            conn.execute(
                "INSERT INTO gc_runs(id, started_at, status) VALUES (?, ?, 'marked')",
                (run_id, now),
            )
            conn.executemany(
                "INSERT INTO gc_candidates(run_id, object_id, blob, state, marked_at)"
                " VALUES (?, ?, ?, 'candidate', ?)",
                [(run_id, oid, all_blobs[oid], now) for oid in candidates],
            )
            # 仍在未提交的标记事务内：此处崩溃注入（如 SIGKILL）会让整笔
            # 标记随事务回滚，数据库回到标记前状态。
            self.hooks.mark_before_commit(run_id, candidates)
        self.hooks.mark_end(run_id, candidates)
        return GCMarkResult(run_id=run_id, candidate_ids=candidates)

    def gc_sweep(self, run_id: str) -> GCSweepResult:
        """第二阶段：逐个复核候选并删除确认无引用者。

        复核依据的“当前”语义是**标记时刻**：标记时仍有效的租约在复核时继续
        有效（租约可以在窗口内到期，但这属于下一次回收的处理范围），从而保证
        回收绝不会删除“标记时仍被根引用”的对象。快照引用没有时间限制，始终保活。

        每个候选在独立的 IMMEDIATE 写事务中完成复核与元数据删除。
        两阶段之间获得新快照/租约引用的候选转为 ``rescued``。
        进程中断后重复调用本方法是安全的幂等重试。
        """

        conn = self._connect()
        try:
            run = conn.execute(
                "SELECT status, started_at FROM gc_runs WHERE id = ?", (run_id,)
            ).fetchone()
            if run is None:
                raise UnknownGCRunError(f"回收运行不存在: {run_id}")
            mark_time = run["started_at"]
            marked = [
                r[0]
                for r in conn.execute(
                    "SELECT object_id FROM gc_candidates WHERE run_id = ?"
                    " ORDER BY object_id",
                    (run_id,),
                ).fetchall()
            ]
        finally:
            conn.close()

        self.hooks.sweep_begin(run_id, marked)

        for oid in marked:
            self._sweep_one(run_id, oid, mark_time)

        finish_now = self._now()
        with self._tx() as conn:
            conn.execute(
                "UPDATE gc_runs SET status = 'swept', finished_at = ? WHERE id = ?",
                (finish_now, run_id),
            )
            reclaimed = [
                r[0]
                for r in conn.execute(
                    "SELECT object_id FROM gc_candidates"
                    " WHERE run_id = ? AND state = 'reclaimed' ORDER BY object_id",
                    (run_id,),
                ).fetchall()
            ]
            rescued = [
                r[0]
                for r in conn.execute(
                    "SELECT object_id FROM gc_candidates"
                    " WHERE run_id = ? AND state = 'rescued' ORDER BY object_id",
                    (run_id,),
                ).fetchall()
            ]
        return GCSweepResult(run_id=run_id, reclaimed=reclaimed, rescued=rescued)

    def gc(self, *, run_id: str | None = None) -> GCSweepResult:
        """便捷方法：顺序执行标记与复核，返回复核结果。"""

        mark = self.gc_mark(run_id=run_id)
        return self.gc_sweep(mark.run_id)

    def _sweep_one(self, run_id: str, object_id: str, mark_time: int) -> None:
        """复核并处理一个候选。安全重试：任何中途崩溃都可重入。

        删除一律针对**候选记录的物理化身** ``blob``，与逻辑 id 当前是否又有行
        （同内容复活）无关。提交状态下删除顺序严格为
        「先删元数据并提交，再删旧化身文件」：

        * 崩溃在提交之前：事务整体回滚，文件、行、候选都保持原状，重试即可；
        * 崩溃在提交之后、删文件之前：旧化身成为由 reclaimed 候选跟踪的待清理
          文件，重试时补删。即使此时同内容已复活成新化身/新快照，补删的也只是
          旧文件，新化身与新快照不受影响，任何 API 仍能读到内容。
        """

        reclaimed_at = self._now()
        pending_path: Path | None = None
        fire_after_hook = False
        with self._tx() as conn:
            candidate = conn.execute(
                "SELECT state, blob FROM gc_candidates"
                " WHERE run_id = ? AND object_id = ?",
                (run_id, object_id),
            ).fetchone()
            if candidate is None:
                return
            old_blob = candidate["blob"]
            old_path = self._path_for_blob(object_id, old_blob)

            if candidate["state"] == "reclaimed":
                # 上一轮元数据已提交、旧文件可能还没删（或已删）。
                # 同一逻辑 id 此刻可能已复活成**新化身**（objects 里又有行）：
                # 只要行指向的不是本候选记录的旧化身，就是合法复活，我们只补删
                # 旧文件、绝不动新行；只有行仍指向同一个已判删旧化身才是不变量
                # 被破坏（那样补删文件会让行悬空）。
                row = conn.execute(
                    "SELECT blob FROM objects WHERE id = ?", (object_id,)
                ).fetchone()
                if row is not None and row["blob"] == old_blob:
                    raise InternalConsistencyError(
                        f"reclaimed 候选的化身仍被对象行引用: {object_id}"
                        f" (blob {old_blob})"
                    )
                if old_path.exists():
                    pending_path = old_path
                fire_after_hook = pending_path is not None
            elif candidate["state"] == "candidate":
                obj_row = conn.execute(
                    "SELECT blob FROM objects WHERE id = ?", (object_id,)
                ).fetchone()
                if obj_row is not None and obj_row["blob"] != old_blob:
                    # 同一逻辑 id 已复活成**新化身**（旧行被删后重新 put）。
                    # 新行可能已被新快照/租约引用，也可能尚未被引用：无论哪种，
                    # 本次旧回收都只清理自己那代旧文件、绝不能删新行。候选记为
                    # reclaimed（针对标记时那一代），审计上可凭 object_id 与新行
                    # 的 blob 不同识别“该对象在回收窗口里被同内容复活过”。
                    conn.execute(
                        "UPDATE gc_candidates SET state = 'reclaimed',"
                        " reclaimed_at = ? WHERE run_id = ? AND object_id = ?",
                        (reclaimed_at, run_id, object_id),
                    )
                    if old_path.exists():
                        pending_path = old_path
                    fire_after_hook = True
                elif obj_row is None:
                    # 行已不在（旧化身未被复活）：落审计状态，旧文件事务外补删。
                    conn.execute(
                        "UPDATE gc_candidates SET state = 'reclaimed',"
                        " reclaimed_at = ? WHERE run_id = ? AND object_id = ?",
                        (reclaimed_at, run_id, object_id),
                    )
                    if old_path.exists():
                        pending_path = old_path
                    fire_after_hook = True
                elif self._is_live(conn, object_id, mark_time):
                    # 仍是同一化身，且被快照引用（始终保活），或被在标记时刻仍未
                    # 到期的租约引用，又或两阶段之间/本次扫描期间获得的新根引用：
                    # 一律救回，文件与行都保留。
                    conn.execute(
                        "UPDATE gc_candidates SET state = 'rescued' WHERE run_id = ?"
                        " AND object_id = ?",
                        (run_id, object_id),
                    )
                else:
                    # 同一化身、复核通过：钩子在写锁持有期间触发，因此并发发布会
                    # 阻塞到「行删除 + reclaimed 落库」提交完成；提交后这一代对新
                    # 事务不可见——不重新 put 的引用确定性收到对象不存在错误，
                    # 重新 put 的复活则进入上面的新化身分支而被放行。
                    self.hooks.before_reclaim(run_id, object_id)
                    # 外键 RESTRICT 是最后一道闸：若存在引用，提交直接失败。
                    conn.execute("DELETE FROM objects WHERE id = ?", (object_id,))
                    conn.execute(
                        "UPDATE gc_candidates SET state = 'reclaimed',"
                        " reclaimed_at = ? WHERE run_id = ? AND object_id = ?",
                        (reclaimed_at, run_id, object_id),
                    )
                    if old_path.exists():
                        pending_path = old_path
                    fire_after_hook = True
            # rescued 候选：什么都不做。

        # 事务已提交（或候选已被救回）；现在才删除不再被任何元数据引用的旧化身。
        # 这里是“元数据已提交、文件尚未 unlink”的复活窗口：在 unlink 前一刻触发
        # 钩子，测试可在此让客户端重新 put + 发布快照；因文件名按化身隔离，下面的
        # unlink 只会删掉旧文件，碰不到复活写入的新文件。崩溃在这里只会留下待
        # 清理文件，重试本方法时按 reclaimed 分支补删（同内容复活也只补删旧化身）。
        if pending_path is not None:
            self.hooks.before_unlink(run_id, object_id, pending_path.name)
            with contextlib.suppress(FileNotFoundError):
                pending_path.unlink()
        if fire_after_hook:
            self.hooks.after_reclaim(run_id, object_id)

    # --------------------------------------------------------------- 内部查询

    @staticmethod
    def _missing_object_ids(conn: sqlite3.Connection, oids: list[str]) -> list[str]:
        if not oids:
            return []
        placeholders = ",".join("?" * len(oids))
        present = {
            r[0]
            for r in conn.execute(
                f"SELECT id FROM objects WHERE id IN ({placeholders})", oids
            ).fetchall()
        }
        return [oid for oid in oids if oid not in present]

    @staticmethod
    def _prune_expired_leases(conn: sqlite3.Connection, now: int) -> None:
        # 级联删除 lease_objects；必须在 IMMEDIATE 事务内调用。
        conn.execute("DELETE FROM leases WHERE expires_at <= ?", (now,))

    @staticmethod
    def _is_live(conn: sqlite3.Connection, object_id: str, now: int) -> bool:
        row = conn.execute(
            """
            SELECT 1 FROM snapshot_objects WHERE object_id = ?
            UNION ALL
            SELECT 1 FROM lease_objects lo
            JOIN leases l ON l.id = lo.lease_id
            WHERE lo.object_id = ? AND l.expires_at > ?
            LIMIT 1
            """,
            (object_id, object_id, now),
        ).fetchone()
        return row is not None

    @staticmethod
    def _live_object_ids(conn: sqlite3.Connection, now: int) -> set[str]:
        live = {
            r[0]
            for r in conn.execute(
                "SELECT DISTINCT object_id FROM snapshot_objects"
            ).fetchall()
        }
        live.update(
            r[0]
            for r in conn.execute(
                """
                SELECT DISTINCT lo.object_id
                FROM lease_objects lo
                JOIN leases l ON l.id = lo.lease_id
                WHERE l.expires_at > ?
                """,
                (now,),
            ).fetchall()
        )
        return live

    # ------------------------------------------------------------- 资源清理

    def close(self) -> None:
        """目前所有连接均为短连接，此方法仅为调用方提供对称的生命周期接口。"""

    def __enter__(self) -> "Repository":
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.close()
