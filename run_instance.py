# -*- coding: utf-8 -*-
"""
读取指定 JSON 实例文件，运行调度，并绘制甘特图。

用法：
  python run_instance.py instances/example1.json
  python run_instance.py instances/example1.json --no-verbose
"""

import sys
import argparse
from scheduler import build_scheduler_from_instance, plot_gantt


def main():
    parser = argparse.ArgumentParser(description="运行 DAG 切分式调度实例")
    parser.add_argument("instance", help="实例 JSON 文件路径")
    parser.add_argument("--no-verbose", action="store_true", help="不打印详细调度过程")
    parser.add_argument("--no-plot", action="store_true", help="不绘制甘特图")
    args = parser.parse_args()

    name, scheduler = build_scheduler_from_instance(args.instance)
    print(f"实例名称：{name}")
    print(f"实例文件：{args.instance}")

    result = scheduler.schedule(verbose=not args.no_verbose)

    print("\n" + "=" * 70)
    print("最终结果摘要")
    print("=" * 70)
    print(f"所有核心最大完成时间 R_cores = {result['makespan_cores']}")
    print(f"汇点完成时间 R_sink = {result['makespan_sink']}")
    print(f"理论下界 LB = {result['lb']}")
    print(f"L_cp = {result['l_cp']}, LB_type = {result['lb_type']}")
    print(f"各类型下界: {result['lb_each']}")
    print(f"公共点: {result['common']}")
    print("注意：LB 是工作量下界，不是完成时间；R_sink 才是实际 WCRT。")
    print("=" * 70)

    if not args.no_plot:
        plot_gantt(scheduler, result, title=f"{name} - 切分式全局调度甘特图")


if __name__ == "__main__":
    main()