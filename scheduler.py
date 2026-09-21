# -*- coding: utf-8 -*-
"""
面向类型受限 DAG 任务的切分式全局调度方法
——修复版 v3：主动切分 + 瓶颈类型每核工作量均衡 + 事件驱动

本文件只包含：
  - Task 类
  - SplitScheduler 类
  - plot_gantt 可视化函数
  - load_instance_from_json / build_scheduler_from_instance 工具函数

实例请写在单独的 JSON 文件中，由 run_instance.py 读取。
"""

from collections import defaultdict, deque
import json
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches

try:
    plt.rcParams['font.sans-serif'] = ['SimHei', 'Microsoft YaHei', 'Arial Unicode MS']
    plt.rcParams['axes.unicode_minus'] = False
except Exception:
    pass


class Task:
    def __init__(self, vid, ctype, wcet, is_common=False):
        self.vid = vid
        self.ctype = ctype
        self.wcet = wcet
        self.is_common = is_common
        self.remaining = wcet
        self.segments = []           # [(core_id, start, end)]
        self.start = None
        self.finish = None
        self.latest_start = None

    def done(self):
        return self.remaining <= 1e-9

    def __repr__(self):
        return f"{self.vid}(type={self.ctype}, wcet={self.wcet})"


class SplitScheduler:
    def __init__(self, tasks, edges, core_counts):
        self.tasks = tasks
        self.edges = edges
        self.core_counts = core_counts

        self.succ = defaultdict(list)
        self.pred = defaultdict(list)
        for u, v in edges:
            self.succ[u].append(v)
            self.pred[v].append(u)

        self.indegree = {vid: len(self.pred[vid]) for vid in tasks}

        self.cores = {}
        for t, m in core_counts.items():
            self.cores[t] = [(t, i) for i in range(m)]

        self.core_avail = {}
        self.core_timeline = defaultdict(list)
        self.core_load = {}          # 每个核心累计已排工作量
        for t, m in core_counts.items():
            for i in range(m):
                self.core_avail[(t, i)] = 0.0
                self.core_load[(t, i)] = 0.0

    # ---------------- 图算法 ----------------
    def topological_order(self):
        indeg = dict(self.indegree)
        q = deque([v for v in self.tasks if indeg[v] == 0])
        order = []
        while q:
            u = q.popleft()
            order.append(u)
            for v in self.succ[u]:
                indeg[v] -= 1
                if indeg[v] == 0:
                    q.append(v)
        if len(order) != len(self.tasks):
            raise ValueError("DAG 中存在环")
        return order

    def source_sink(self):
        sources = [v for v in self.tasks if len(self.pred[v]) == 0]
        sinks = [v for v in self.tasks if len(self.succ[v]) == 0]
        return sources, sinks

    def analyze_lower_bound(self):
        """
        一次性完成：标记公共点、计算 L_cp、计算 LB_type。
        """
        order = self.topological_order()
        sources, sinks = self.source_sink()

        # 公共点识别：删除 v 后，从源点出发能否到达任何汇点
        common = set()
        for v in self.tasks:
            if v in sources or v in sinks:
                common.add(v)
                continue
            visited = set(sources)
            q = deque(sources)
            while q:
                u = q.popleft()
                for w in self.succ[u]:
                    if w == v:
                        continue
                    if w not in visited:
                        visited.add(w)
                        q.append(w)
            if not any(s in visited for s in sinks):
                common.add(v)

        for v in common:
            self.tasks[v].is_common = True

        # L_cp：去除公共点 WCET 后的最长路径
        dist = {v: 0.0 for v in self.tasks}
        for u in order:
            w = 0.0 if self.tasks[u].is_common else self.tasks[u].wcet
            for v in self.succ[u]:
                if dist[u] + w > dist[v]:
                    dist[v] = dist[u] + w

        best = 0.0
        for v in sinks:
            w = 0.0 if self.tasks[v].is_common else self.tasks[v].wcet
            best = max(best, dist[v] + w)
        l_cp = best

        # LB_type：去除公共点负载
        load = defaultdict(float)
        for task in self.tasks.values():
            if task.is_common:
                continue
            load[task.ctype] += task.wcet
        lb_each = {}
        for t, total in load.items():
            m = self.core_counts.get(t, 1)
            lb_each[t] = total / m
        lb_type = max(lb_each.values()) if lb_each else 0.0

        return l_cp, lb_type, lb_each, sorted(common)

    def compute_latest_start(self, lb):
        """
        latest_start(v) = lb - (v 到汇点的最长剩余路径)。
        只用于优先级提升，不禁止切分。
        """
        order = self.topological_order()
        rdist = {v: self.tasks[v].wcet for v in self.tasks}
        for u in reversed(order):
            for v in self.succ[u]:
                if rdist[v] + self.tasks[u].wcet > rdist[u]:
                    rdist[u] = rdist[v] + self.tasks[u].wcet
        for v in self.tasks:
            self.tasks[v].latest_start = lb - rdist[v]

    # ---------------- 调度核心 ----------------
    def ready_tasks(self, finished):
        """所有前驱已完成、且自身尚未完成的任务"""
        ready = []
        for vid, task in self.tasks.items():
            if task.done():
                continue
            if all(p in finished for p in self.pred[vid]):
                ready.append(vid)
        return ready

    def find_gap(self, core, start_time):
        """
        在 core 的时间线上，找从 start_time 开始的下一个空闲区间。
        返回 (gap_start, gap_end)。若 start_time 之后没有已排片段，gap_end=inf。
        """
        timeline = sorted(self.core_timeline[core])
        cur = start_time
        for (s, e) in timeline:
            if e <= cur + 1e-9:
                continue
            if s > cur + 1e-9:
                return cur, s
            cur = max(cur, e)
        return cur, float('inf')

    def schedule(self, verbose=True):
        order = self.topological_order()
        sources, sinks = self.source_sink()

        l_cp, lb_type, lb_each, common = self.analyze_lower_bound()
        lb = max(l_cp, lb_type)
        self.compute_latest_start(lb)

        if verbose:
            print("=" * 70)
            print("DAG 调度分析")
            print("=" * 70)
            print(f"拓扑序: {order}")
            print(f"源点: {sources}, 汇点: {sinks}")
            print(f"公共点(不参与下界): {common}")
            print(f"关键路径(去公共点 WCET)长度 L_cp = {l_cp}")
            print(f"各类型负载下界(去公共点): {lb_each}")
            print(f"类型负载下界 LB_type = {lb_type}")
            print(f"综合下界 LB = {lb}")
            if l_cp > lb_type:
                print("判断：L_cp > LB_type -> 情形 A（关键路径是瓶颈）")
            elif l_cp < lb_type:
                print("判断：L_cp < LB_type -> 情形 B（类型负载是瓶颈）")
            else:
                print("判断：L_cp == LB_type -> 两种因素均衡")
            print("-" * 70)

        if l_cp > lb_type:
            priority_mode = "critical"
            bottleneck_type = None
        else:
            priority_mode = "bottleneck"
            bottleneck_type = max(lb_each, key=lambda t: lb_each[t])

        if verbose:
            print(f"调度策略: {priority_mode}", end="")
            if bottleneck_type is not None:
                print(f", 瓶颈类型 = {bottleneck_type}")
            else:
                print(", 关键路径优先")
            print("-" * 70)

        finished = set()
        running = set()
        current_time = 0.0

        while len(finished) < len(self.tasks):
            # 1) 把 current_time 之前已完成的任务标记为 finished
            for vid in list(running):
                task = self.tasks[vid]
                if task.done() and task.finish is not None and task.finish <= current_time + 1e-9:
                    finished.add(vid)
                    running.discard(vid)

            # 2) 获取就绪任务，并按优先级排序；latest_start 到达的任务优先
            ready = self.ready_tasks(finished)
            forced = []
            normal = []
            for vid in ready:
                task = self.tasks[vid]
                ls = task.latest_start
                if ls is not None and current_time >= ls - 1e-9:
                    forced.append(vid)
                else:
                    normal.append(vid)

            if priority_mode == "critical":
                normal.sort(key=lambda v: order.index(v))
            else:
                normal.sort(key=lambda v: (
                    0 if self.tasks[v].ctype == bottleneck_type else 1,
                    order.index(v)
                ))
            ready_sorted = forced + normal

            # 3) 尝试为就绪任务分配核心，能并行就并行
            assigned_any = False
            for vid in ready_sorted:
                task = self.tasks[vid]
                if task.done():
                    continue
                ctype = task.ctype
                remaining = task.remaining

                # 前驱完成时间
                pred_finish = max(
                    (self.tasks[p].finish for p in self.pred[vid]
                     if self.tasks[p].finish is not None),
                    default=0.0
                )

                # 选择核心：
                # - 瓶颈类型任务：优先选累计工作量最少的核心
                # - 其他类型：优先选最早可用的核心
                candidate_cores = self.cores[ctype]
                if priority_mode == "bottleneck" and ctype == bottleneck_type:
                    candidate_cores = sorted(
                        candidate_cores,
                        key=lambda c: (self.core_load[c], self.core_avail[c])
                    )
                else:
                    candidate_cores = sorted(
                        candidate_cores,
                        key=lambda c: self.core_avail[c]
                    )

                # 选一个能容纳任务的 core（允许切分）
                chosen = None
                for core in candidate_cores:
                    gap_start, gap_end = self.find_gap(core, current_time)
                    start_time = max(current_time, pred_finish, gap_start)
                    # 核心在 start_time 之后有可用区间即可
                    if gap_end > start_time + 1e-9:
                        chosen = (core, start_time, gap_end)
                        break
                if chosen is None:
                    continue

                core, start_time, gap_end = chosen
                gap_len = gap_end - start_time if gap_end != float('inf') else float('inf')

                # 主动切分判断：
                # 对瓶颈类型，如果该核心当前负载 + remaining 会超过 LB_type，
                # 就只排 (LB_type - core_load) 这么多，剩余部分留给其他核心/后续时间。
                if priority_mode == "bottleneck" and ctype == bottleneck_type:
                    target = lb_type
                    allowed = target - self.core_load[core]
                    if allowed < remaining - 1e-9 and allowed > 1e-9:
                        # 切分：前 allowed 排到当前核心，剩余排到后续
                        seg1 = allowed
                        seg2 = remaining - allowed
                        task.segments.append((core, start_time, start_time + seg1))
                        self.core_timeline[core].append((start_time, start_time + seg1))
                        self.core_avail[core] = max(self.core_avail[core], start_time + seg1)
                        self.core_load[core] += seg1
                        task.remaining = seg2
                        if task.start is None:
                            task.start = start_time
                        task.finish = max(task.finish or 0.0, start_time + seg1)
                        running.add(vid)
                        assigned_any = True
                        if verbose:
                            print(f"切分 {vid}: 在核心 {core} 执行 {seg1:.2f}，剩余 {seg2:.2f}")
                        continue

                # 普通切分：核心空洞小于 remaining，且不是无限
                if gap_end != float('inf'):
                    gap_len = gap_end - start_time
                    if 1e-9 < gap_len < remaining - 1e-9:
                        seg1 = gap_len
                        seg2 = remaining - gap_len
                        task.segments.append((core, start_time, start_time + seg1))
                        self.core_timeline[core].append((start_time, start_time + seg1))
                        self.core_avail[core] = max(self.core_avail[core], start_time + seg1)
                        self.core_load[core] += seg1
                        task.remaining = seg2
                        if task.start is None:
                            task.start = start_time
                        task.finish = max(task.finish or 0.0, start_time + seg1)
                        running.add(vid)
                        assigned_any = True
                        if verbose:
                            print(f"切分 {vid}: 在核心 {core} 执行 {seg1:.2f}，剩余 {seg2:.2f}")
                        continue

                # 不切分：直接执行完剩余工作量
                task.segments.append((core, start_time, start_time + remaining))
                self.core_timeline[core].append((start_time, start_time + remaining))
                self.core_avail[core] = max(self.core_avail[core], start_time + remaining)
                self.core_load[core] += remaining
                task.remaining = 0
                if task.start is None:
                    task.start = start_time
                task.finish = max(task.finish or 0.0, start_time + remaining)
                running.add(vid)
                assigned_any = True
                if verbose:
                    print(f"执行 {vid}: 核心 {core}, 开始 {start_time:.2f}, "
                          f"结束 {start_time + remaining:.2f}, WCET={task.wcet}")

            # 4) 推进时间：本轮没有任何分配时才推进
            if not assigned_any:
                event_times = []
                for v in running:
                    if self.tasks[v].finish is not None:
                        event_times.append(self.tasks[v].finish)
                for task in self.tasks.values():
                    for (_, s, _) in task.segments:
                        if s > current_time + 1e-9:
                            event_times.append(s)
                if event_times:
                    current_time = min(event_times)
                else:
                    if len(finished) < len(self.tasks):
                        raise RuntimeError(
                            f"调度停滞：current_time={current_time}, "
                            f"finished={sorted(finished)}, "
                            f"remaining={[v for v in self.tasks if not self.tasks[v].done()]}"
                        )
                    break
            # 若有分配，不推进时间，继续下一轮尝试分配更多任务

        makespan_cores = max(self.core_avail.values()) if self.core_avail else 0.0
        sink_vid = sinks[0] if sinks else None
        makespan_sink = self.tasks[sink_vid].finish if sink_vid and self.tasks[sink_vid].finish is not None else 0.0

        if verbose:
            print("-" * 70)
            print("调度结果：")
            for vid in order:
                task = self.tasks[vid]
                common_flag = " [公共点]" if task.is_common else ""
                print(f"  {vid}: type={task.ctype}, WCET={task.wcet}{common_flag}, "
                      f"segments={task.segments}, start={task.start}, finish={task.finish}")
            print("-" * 70)
            print("各核心时间线：")
            for core in sorted(self.core_timeline.keys()):
                print(f"  {core}: {self.core_timeline[core]}")
            print("-" * 70)
            print(f"所有核心最大完成时间 R_cores = {makespan_cores}")
            print(f"汇点完成时间 R_sink = {makespan_sink}")
            print(f"理论下界 LB = {lb}")
            print("注意：LB 是工作量下界，不是完成时间；R_sink 才是实际 WCRT。")

        return {
            "makespan_cores": makespan_cores,
            "makespan_sink": makespan_sink,
            "lb": lb,
            "l_cp": l_cp,
            "lb_type": lb_type,
            "lb_each": lb_each,
            "common": common,
            "schedule": {vid: self.tasks[vid].segments for vid in self.tasks},
            "core_timeline": dict(self.core_timeline),
        }


# ============================================================
# 甘特图可视化
# ============================================================
def plot_gantt(scheduler, result, title="面向类型受限 DAG 任务的切分式全局调度甘特图"):
    core_timeline = result["core_timeline"]
    tasks = scheduler.tasks

    cores = sorted(core_timeline.keys(), key=lambda c: (c[0], c[1]))
    if not cores:
        print("没有可绘制的核心时间线")
        return

    vids = sorted(tasks.keys(), key=lambda v: (len(v), v))
    cmap = plt.get_cmap("tab20")
    color_map = {vid: cmap(i % 20) for i, vid in enumerate(vids)}

    fig, ax = plt.subplots(figsize=(12, 6))
    y_pos = {core: i for i, core in enumerate(cores)}
    bar_height = 0.6

    for core in cores:
        y = y_pos[core]
        for (vid, segments) in result["schedule"].items():
            for (seg_core, start, end) in segments:
                if seg_core != core:
                    continue
                duration = end - start
                if duration <= 0:
                    continue
                ax.barh(
                    y, duration, left=start, height=bar_height,
                    color=color_map.get(vid, "gray"),
                    edgecolor="black", linewidth=0.8, alpha=0.9
                )
                ax.text(
                    start + duration / 2, y, vid,
                    ha="center", va="center",
                    fontsize=8, color="white", weight="bold"
                )

    ax.set_yticks(list(y_pos.values()))
    ax.set_yticklabels([f"类型{t} 核心{i}" for (t, i) in cores])
    ax.set_xlabel("时间")
    ax.set_ylabel("核心")
    ax.set_title(title)

    lb = result["lb"]
    ax.axvline(lb, color="red", linestyle="--", linewidth=1.5, label=f"LB={lb:.2f}")
    sink_time = result["makespan_sink"]
    ax.axvline(sink_time, color="green", linestyle=":", linewidth=1.5,
               label=f"R_sink={sink_time:.2f}")
    cores_time = result["makespan_cores"]
    ax.axvline(cores_time, color="blue", linestyle="-.", linewidth=1.5,
               label=f"R_cores={cores_time:.2f}")

    legend_patches = [
        mpatches.Patch(color=color_map[vid],
                       label=f"{vid}(类型{tasks[vid].ctype}, WCET={tasks[vid].wcet})")
        for vid in vids
    ]
    handles, labels = ax.get_legend_handles_labels()
    ax.legend(handles=legend_patches + handles, loc="upper right",
              fontsize=8, ncol=2, framealpha=0.9)

    max_time = max(
        [end for segs in result["schedule"].values() for (_, _, end) in segs] +
        [lb, sink_time, cores_time]
    )
    ax.set_xlim(0, max_time * 1.1)
    ax.grid(axis="x", linestyle=":", alpha=0.5)

    plt.tight_layout()
    plt.show()


# ============================================================
# 从 JSON 读取实例
# ============================================================
def load_instance_from_json(path):
    """
    JSON 格式示例：
    {
      "name": "example1",
      "tasks": [
        {"vid": "v1", "ctype": 2, "wcet": 8, "is_common": true},
        ...
      ],
      "edges": [["v1","v2"], ...],
      "core_counts": {"1": 2, "2": 2}
    }
    说明：
      - ctype 用整数表示类型编号；
      - core_counts 的 key 在 JSON 里是字符串，读入后转成 int；
      - is_common 可省略，默认 false。
    """
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)

    tasks = {}
    for item in data["tasks"]:
        vid = item["vid"]
        ctype = int(item["ctype"])
        wcet = float(item["wcet"])
        is_common = bool(item.get("is_common", False))
        tasks[vid] = Task(vid, ctype, wcet, is_common=is_common)

    edges = [tuple(e) for e in data.get("edges", [])]

    core_counts = {int(k): int(v) for k, v in data.get("core_counts", {}).items()}

    name = data.get("name", "unnamed_instance")
    return name, tasks, edges, core_counts


def build_scheduler_from_instance(path):
    name, tasks, edges, core_counts = load_instance_from_json(path)
    scheduler = SplitScheduler(tasks, edges, core_counts)
    return name, scheduler