"""
run_demos —— 一键运行所有协议演示

用法：
    python run_demos.py            # 运行全部
    python run_demos.py websocket  # 只运行某一个

每个协议的 demo() 都会打印报文的真实字节，是学习协议最好的方式。
"""

import sys
import traceback

MODULES = ["udp", "tcp", "http", "websocket", "dns", "discovery"]


def run_one(name: str) -> bool:
    """运行单个模块的演示，返回是否成功"""
    print()
    print("█" * 70)
    print(f"█  {name}.py")
    print("█" * 70)
    try:
        module = __import__(f"protocols.{name}", fromlist=["demo"])
        module.demo()
        return True
    except Exception as exc:
        print(f"\n❌ {name}.py 演示失败：{type(exc).__name__}: {exc}")
        traceback.print_exc()
        return False


def main():
    # 允许指定只跑某几个模块
    targets = sys.argv[1:] or MODULES

    results = {}
    for name in targets:
        results[name] = run_one(name)

    print()
    print("=" * 70)
    print("运行结果汇总")
    print("=" * 70)
    ok = 0
    for name, success in results.items():
        status = "✅ 通过" if success else "❌ 失败"
        print(f"  {name:<12} {status}")
        ok += 1 if success else 0
    print(f"\n共 {len(results)} 个，通过 {ok} 个，失败 {len(results) - ok} 个")
    return 0 if ok == len(results) else 1


if __name__ == "__main__":
    sys.exit(main())
