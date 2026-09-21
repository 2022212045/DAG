# -*- coding: utf-8 -*-
"""
读取 DAG 实例 JSON，绘制类型化任务拓扑图，并保存为图片。

用法：
  python plot_dag.py instances/example1.json
  python plot_dag.py instances/example1.json --out example1.png
  python plot_dag.py instances/example1.json --no-show
"""

import json
import argparse
import networkx as nx
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches

try:
    plt.rcParams['font.sans-serif'] = ['SimHei', 'Microsoft YaHei', 'Arial Unicode MS']
    plt.rcParams['axes.unicode_minus'] = False
except Exception:
    pass


# ============================================================
# 读取 JSON 实例
# ============================================================
def load_instance(path):
    """
    返回：
      name        : 实例名
      vertices    : 顶点列表（按 JSON 中 tasks 顺序）
      wcet        : {vid: wcet}
      vtype       : {vid: ctype}
      edges       : [(u, v), ...]
    """
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)

    vertices = []
    wcet = {}
    vtype = {}
    for item in data["tasks"]:
        vid = item["vid"]
        vertices.append(vid)
        wcet[vid] = float(item["wcet"])
        vtype[vid] = int(item["ctype"])

    edges = [tuple(e) for e in data.get("edges", [])]
    name = data.get("name", "unnamed_instance")
    return name, vertices, wcet, vtype, edges


# ============================================================
# 自动分层（最长路径分层）
# ============================================================
def compute_layers(vertices, edges):
    """
    按最长路径给节点分层：
      源点 layer = 0
      其他节点 layer = max(pred layer) + 1
    返回：
      layers : {layer_id: [vid, ...]}  按层号升序
    """
    G = nx.DiGraph()
    G.add_nodes_from(vertices)
    G.add_edges_from(edges)

    if not nx.is_directed_acyclic_graph(G):
        raise ValueError("输入不是 DAG（存在环）")

    order = list(nx.topological_sort(G))
    layer = {v: 0 for v in vertices}
    for u in order:
        for v in G.successors(u):
            if layer[u] + 1 > layer[v]:
                layer[v] = layer[u] + 1

    layers = {}
    for v in vertices:
        layers.setdefault(layer[v], []).append(v)
    return dict(sorted(layers.items()))


# ============================================================
# 绘制
# ============================================================
def plot_dag(name, vertices, wcet, vtype, edges, out_path=None, show=True):
    G = nx.DiGraph()
    G.add_nodes_from(vertices)
    G.add_edges_from(edges)

    # 分层
    layers = compute_layers(vertices, edges)

    # 计算坐标：层号决定 y，层内均匀分布决定 x
    pos = {}
    y_step = 1.0
    for layer_id, nodes in layers.items():
        n = len(nodes)
        y = -layer_id * y_step
        for idx, node in enumerate(nodes):
            x = (idx + 1) / (n + 1)
            pos[node] = (x, y)

    # 节点颜色：类型 1 = 蓝，类型 2 = 红，其他 = 灰
    def color_of(t):
        if t == 1:
            return '#4A90D9'
        elif t == 2:
            return '#E74C3C'
        else:
            return '#7F8C8D'

    node_colors = [color_of(vtype[v]) for v in vertices]

    # 节点标签：编号 + WCET
    labels = {v: f"{v}\n(C={wcet[v]:g})" for v in vertices}

    fig, ax = plt.subplots(figsize=(14, 8))

    nx.draw_networkx_edges(
        G, pos, ax=ax,
        edge_color='#555555',
        arrows=True,
        arrowstyle='-|>',
        arrowsize=25,
        width=1.8,
        connectionstyle='arc3,rad=0.08',
        min_target_margin=18
    )

    nx.draw_networkx_nodes(
        G, pos, ax=ax,
        node_color=node_colors,
        node_size=1800,
        edgecolors='black',
        linewidths=1.5
    )

    nx.draw_networkx_labels(
        G, pos, labels, ax=ax,
        font_size=9,
        font_color='white',
        font_weight='bold'
    )

    # 图例：只显示出现过的类型
    present_types = sorted(set(vtype.values()))
    legend_elements = []
    for t in present_types:
        legend_elements.append(
            mpatches.Patch(color=color_of(t), label=f'类型 {t}')
        )
    if legend_elements:
        ax.legend(handles=legend_elements, loc='upper right', fontsize=11)

    ax.set_title(f"{name} - Typed DAG Topology", fontsize=15, fontweight='bold')
    ax.axis('off')
    plt.tight_layout()

    if out_path:
        plt.savefig(out_path, dpi=200, bbox_inches='tight')
        print(f"图片已保存：{out_path}")

    if show:
        plt.show()
    else:
        plt.close(fig)


# ============================================================
# 主程序
# ============================================================
def main():
    parser = argparse.ArgumentParser(description="读取 DAG JSON，绘制并保存拓扑图")
    parser.add_argument("instance", help="实例 JSON 文件路径")
    parser.add_argument("--out", default=None, help="输出图片路径（默认 <实例名>.png）")
    parser.add_argument("--no-show", action="store_true", help="不弹窗显示，只保存")
    args = parser.parse_args()

    name, vertices, wcet, vtype, edges = load_instance(args.instance)

    out_path = args.out
    if out_path is None:
        out_path = f"{name}.png"

    plot_dag(name, vertices, wcet, vtype, edges,
             out_path=out_path, show=not args.no_show)


if __name__ == "__main__":
    main()