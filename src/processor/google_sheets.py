import gspread
import pickle
import os
import webbrowser
from tkinter import messagebox
from google_auth_oauthlib.flow import InstalledAppFlow
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request

from utils.auth_utils import get_google_creds

def load_gsheet_data(sheet_name):
    """加载指定Google Sheet并返回SKU到cost的字典"""
    try:
        print(f"\n[Google Sheet] 开始加载 {sheet_name} 数据")
        
        # 复用现有认证流程
        creds = get_google_creds()
        client = gspread.authorize(creds)
        
        # 打开指定名称的工作表
        spreadsheet = client.open(sheet_name)
        sheet = spreadsheet.sheet1
        
        # 获取全部数据（包含标题）
        rows = sheet.get_all_values()
        if not rows:
            print(f"[警告] {sheet_name} 表中无数据")
            return {}
        
        # 验证列结构
        if len(rows[0]) < 11:  # 确保至少有11列
            raise ValueError(f"{sheet_name} 表结构错误：需要至少11列")
        
        # 构建SKU-Cost映射
        cost_mapping = {}
        for row in rows[1:]:  # 跳过标题行
            sku = row[0].strip()  # A列
            cost_str = row[10].strip()  # K列（第11列）
            
            if not sku:
                continue
                
            try:
                cost = float(cost_str) if cost_str else 0.0
            except ValueError:
                print(f"[警告] {sheet_name} 表中无效数值：SKU={sku}, 值='{cost_str}'")
                cost = 0.0
                
            cost_mapping[sku] = cost
        
        print(f"成功加载 {len(cost_mapping)} 条 {sheet_name} 数据")
        return cost_mapping
        
    except Exception as e:
        error_msg = f"加载 {sheet_name} 失败：{str(e)}\n"
        error_msg += "请检查：\n- 表格名称是否正确\n- 表格是否已分享给您的账号\n- 网络连接是否正常"
        messagebox.showerror("Google Sheet错误", error_msg)
        return {}

def add_master_sku_from_gsheet(df):
    """从Google Sheet获取SKU映射（OAuth修正版）"""
    try:
        print("\n[Google Sheet] 开始加载SKU映射表")
        
        # 获取用户凭据
        creds = get_google_creds()
        client = gspread.authorize(creds)
        
        # ==== 修改点1：移除服务账号相关提示 ====
        spreadsheet = client.open("SKU Manual Mapping")
        sheet = spreadsheet.sheet1
        
        # ==== 修改点2：增强列名验证 ====
        headers = sheet.row_values(1)
        required_columns = ['channel_sku', 'sku_backup']
        
        # 严格检查列名（忽略大小写和空格）
        header_clean = [h.strip().lower() for h in headers]
        missing = [
            col for col in required_columns 
            if col not in header_clean
        ]
        
        if missing:
            # 生成友好的列名建议
            suggestions = [
                f"现有列：{headers}\n"
                f"需要列：{required_columns}\n"
                f"可能原因：\n"
                f"- 列名拼写错误（检查大小写和空格）\n"
                f"- 表格未使用标准模板"
            ]
            raise ValueError("\n".join(suggestions))
        
        # ==== 修改点3：优化数据加载 ====
        records = sheet.get_all_records()
        sku_mapping = {}
        
        for idx, row in enumerate(records, start=2):
            # 统一处理空值和类型
            channel_sku = str(row.get('channel_sku', '')).strip()
            sku_backup = str(row.get('sku_backup', '')).strip()
            
            if not channel_sku:
                print(f"[跳过] 第{idx}行：channel_sku为空")
                continue
                
            # 重复检查
            if channel_sku in sku_mapping:
                print(f"[警告] 重复channel_sku：{channel_sku} → 将覆盖前值")
                
            sku_mapping[channel_sku] = sku_backup
        
        print(f"成功加载 {len(sku_mapping)} 条映射")
        df['master_sku'] = df['sku'].map(sku_mapping)
        
        return df

    except gspread.exceptions.APIError as e:
        # ==== 修改点4：精准识别权限问题 ====
        error_msg = f"访问Google Sheet失败：{e.response.text}"
        if "PERMISSION_DENIED" in str(e):
            error_msg += "\n请确认：\n1. 已把表格分享给您的Google账号\n2. 表格ID正确"
        messagebox.showerror("权限错误", error_msg)
        return df
        
    except Exception as e:
        messagebox.showwarning("数据处理错误",
            f"SKU匹配异常：{str(e)}\n"
            "将继续使用原始SKU数据")
        return df