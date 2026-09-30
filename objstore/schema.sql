-- 本地对象仓库的 SQLite 元数据模式。
--
-- 逻辑对象 (objects) 以内容哈希为 id；其物理内容存放在对象目录下的一个
-- “化身文件”中，路径为 objects/<id前2位>/<id后62位>-<一次性随机 nonce>。
-- 引入化身名是为了隔离“同内容复活”：一次回收把对象行删掉、但旧文件还没删的
-- 窗口里，客户端可以重新 put 相同内容并立即发布快照——复活会写一个**新名字**
-- 的文件，而旧回收只会 unlink 它标记时记录的**旧名字**文件，两者永不互相删。
-- 快照 (snapshots) 是不可变根集合；租约 (leases) 是带到期时间的临时根。
-- gc_runs / gc_candidates 记录两阶段回收，candidates 刻意不使用外键，
-- 以便对象行被删除后仍保留审计记录。

PRAGMA journal_mode = WAL;
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS schema_meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

INSERT OR IGNORE INTO schema_meta(key, value) VALUES ('version', '2');

CREATE TABLE IF NOT EXISTS objects (
    id          TEXT PRIMARY KEY CHECK (length(id) = 64),
    blob        TEXT NOT NULL UNIQUE CHECK (length(blob) = 32),
    size        INTEGER NOT NULL CHECK (size >= 0),
    created_at  INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS snapshots (
    id         TEXT PRIMARY KEY,
    created_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS snapshot_objects (
    snapshot_id TEXT NOT NULL REFERENCES snapshots(id) ON DELETE CASCADE,
    object_id   TEXT NOT NULL REFERENCES objects(id)   ON DELETE RESTRICT,
    PRIMARY KEY (snapshot_id, object_id)
) WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS leases (
    id         TEXT PRIMARY KEY,
    created_at INTEGER NOT NULL,
    expires_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS lease_objects (
    lease_id  TEXT NOT NULL REFERENCES leases(id) ON DELETE CASCADE,
    object_id TEXT NOT NULL REFERENCES objects(id) ON DELETE RESTRICT,
    PRIMARY KEY (lease_id, object_id)
) WITHOUT ROWID;

CREATE INDEX IF NOT EXISTS idx_leases_expires_at ON leases(expires_at);

CREATE TABLE IF NOT EXISTS gc_runs (
    id           TEXT PRIMARY KEY,
    started_at   INTEGER NOT NULL,
    finished_at  INTEGER,
    status       TEXT NOT NULL CHECK (status IN ('marked', 'swept'))
);

CREATE TABLE IF NOT EXISTS gc_candidates (
    run_id       TEXT NOT NULL REFERENCES gc_runs(id) ON DELETE CASCADE,
    object_id    TEXT NOT NULL,
    -- 标记时刻该对象使用的物理化身名（32 位 hex nonce）。
    -- 复核时即使同内容已复活成新化身，旧回收也只清理这里记录的旧文件；
    -- 候选记为 reclaimed 后，若 objects 中又出现同一 id 的行，该行必须指向
    -- 另一个（复活的新）化身，重试据此判断而非报一致性错误。
    blob         TEXT NOT NULL,
    state        TEXT NOT NULL CHECK (state IN ('candidate', 'reclaimed', 'rescued')),
    marked_at    INTEGER NOT NULL,
    reclaimed_at INTEGER,
    PRIMARY KEY (run_id, object_id)
) WITHOUT ROWID;
