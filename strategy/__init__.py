"""
策略模块
自动注册所有策略类
"""
import sys
from pathlib import Path

# 添加项目根目录到路径
sys.path.insert(0, str(Path(__file__).parent.parent))

# 导入统一B1策略
from strategy.unified_b1_strategy import UnifiedB1Strategy

# 策略类映射（策略注册器会自动扫描，这里仅作为显式导出）
STRATEGIES = {
    'UnifiedB1Strategy': UnifiedB1Strategy,
}

__all__ = [
    'UnifiedB1Strategy',
    'STRATEGIES'
]