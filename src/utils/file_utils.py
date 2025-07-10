import os
import sys

def get_resource_path(relative_path):
    """智能资源路径定位（修复开发模式路径）"""
    if getattr(sys, 'frozen', False):
        base_path = sys._MEIPASS
    else:
        # 确保开发模式路径正确：src目录 -> 父目录（项目根目录）
        base_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    
    full_path = os.path.join(base_path, relative_path)
    print(f"[路径追踪] 资源解析：{full_path}")
    return full_path