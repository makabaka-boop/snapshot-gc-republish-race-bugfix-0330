"""垃圾回收阶段钩子。

钩子只用于可观测性与测试同步（阶段屏障、崩溃注入），绝不参与决策；
回收的正确性不依赖任何钩子被调用。
"""

from __future__ import annotations

from typing import Protocol


class GCHooks(Protocol):
    """回收各阶段的回调协议。所有方法都可以不实现。"""

    def mark_begin(self, run_id: str) -> None:
        """一次新的标记运行开始。"""

    def mark_before_commit(self, run_id: str, candidate_ids: list[str]) -> None:
        """候选已写入但标记事务尚未提交时触发（崩溃注入点）。"""

    def mark_end(self, run_id: str, candidate_ids: list[str]) -> None:
        """候选已在单个事务中落库，尚未删除任何文件。"""

    def sweep_begin(self, run_id: str, candidate_ids: list[str]) -> None:
        """复核阶段开始。此处阻塞即可让并发线程在两阶段之间发布快照/续约。"""

    def before_reclaim(self, run_id: str, object_id: str) -> None:
        """某个候选的复核已通过、删除事务已打开（行删除提交之前）。"""

    def before_unlink(self, run_id: str, object_id: str, blob_name: str) -> None:
        """元数据删除已提交、即将 unlink 旧化身文件前一刻调用。

        这是“元数据已提交、文件尚未删除”的复活窗口：测试在此让客户端重新
        ``put`` 相同内容并发布快照，可复现“同内容复活”竞争。``blob_name`` 是
        本次回收要删的物理化身文件名（形如 ``<id后62位>-<nonce>``）。
        """

    def after_reclaim(self, run_id: str, object_id: str) -> None:
        """候选文件与元数据都已删除后调用。"""


class NullHooks:
    """默认空实现。"""

    def mark_begin(self, run_id: str) -> None:
        pass

    def mark_before_commit(self, run_id: str, candidate_ids: list[str]) -> None:
        pass

    def mark_end(self, run_id: str, candidate_ids: list[str]) -> None:
        pass

    def sweep_begin(self, run_id: str, candidate_ids: list[str]) -> None:
        pass

    def before_reclaim(self, run_id: str, object_id: str) -> None:
        pass

    def before_unlink(self, run_id: str, object_id: str, blob_name: str) -> None:
        pass

    def after_reclaim(self, run_id: str, object_id: str) -> None:
        pass
