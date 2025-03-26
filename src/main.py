import tkinter as tk
from tkinter import filedialog, messagebox
from tkcalendar import Calendar
import pandas as pd
from datetime import datetime
import numpy as np
import os
import sys
import requests
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# ================================ 路径处理函数 ================================
def get_resource_path(relative_path):
    """获取资源文件的路径，兼容开发模式和打包后的exe模式"""
    if getattr(sys, 'frozen', False):
        base_path = sys._MEIPASS
    else:
        base_path = os.path.abspath(".")
    
    # 尝试开发模式路径（src目录的上级）
    dev_path = os.path.join(os.path.dirname(base_path), relative_path)
    if os.path.exists(dev_path):
        return dev_path
    
    full_path = os.path.join(base_path, relative_path)
    print(f"[Debug] 计算出的路径: {full_path}")
    return full_path

# ================================ Google Sheet功能 ================================
def get_master_sku_mapping():
    """从Google Sheet获取SKU映射表（A列:SKU, D列:Master_SKU）"""
    try:
        cred_path = get_resource_path("resources/auth/credentials.json")
        scope = [
            'https://www.googleapis.com/auth/spreadsheets.readonly',
            'https://www.googleapis.com/auth/drive.readonly'
        ]
        
        creds = ServiceAccountCredentials.from_json_keyfile_name(cred_path, scope)
        client = gspread.authorize(creds)
        
        # 修改为你的Google Sheet ID
        sheet_id = "YOUR_GOOGLE_SHEET_ID"
        sheet = client.open_by_key(sheet_id).sheet1
        
        records = sheet.get_all_records(head=1)
        return {str(row['SKU']): str(row['Master_SKU']) for row in records}
    
    except Exception as e:
        messagebox.showwarning("Google Sheet错误",
            f"无法获取SKU映射表:\n{str(e)}\n"
            "程序将继续运行，但master_sku列将为空")
        return {}

# ================================ 核心业务逻辑 ================================
def merge_order_qty(order_df, qty_df, raw_source_df=None):
    """合并Order和QTY数据（新增master_sku列）"""
    try:
        merge_keys = ['order-id', 'shipment-id', 'sku']
        
        for df, name in [(order_df, 'Order'), (qty_df, 'QTY')]:
            missing = [col for col in merge_keys if col not in df.columns]
            if missing:
                raise ValueError(f"{name}表缺少关键列: {', '.join(missing)}")
        
        merged_df = pd.merge(
            order_df,
            qty_df[merge_keys + ['quantity-purchased']],
            on=merge_keys,
            how='left',
            suffixes=('_order', '_qty')
        )
        
        if 'quantity-purchased' in merged_df.columns:
            merged_df.rename(columns={'quantity-purchased': 'QTY'}, inplace=True)
        
        if raw_source_df is not None:
            merged_df = fill_missing_qty(merged_df, raw_source_df)
            
        # 调整列顺序（新增列位置）
        columns_order = [
            'order-id', 'shipment-id', 'sku', 'master_sku',
            'QTY', 'Product Amount', 'Product Tax', 'tax_rate',
            'Shipping', 'Shipping Tax', 'Total_shipping',
            'Giftwrap', 'Giftwrap Tax', 'Total_amount'
        ]
        merged_df = merged_df.reindex(columns=columns_order, errors='ignore')
        
        return merged_df
        
    except Exception as e:
        messagebox.showerror("合并错误", f"合并Order/QTY失败:\n{str(e)}")
        return None

# ================================ GUI处理类 ================================
class AmazonProcessor(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("US Amazon Processor v3.2")
        self.geometry("550x470")
        self.configure(bg="#f0f0f0")
        
        try:
            base_path = sys._MEIPASS if getattr(sys, 'frozen', False) else os.path.dirname(os.path.abspath(__file__))
            icon_path = os.path.join(base_path, "resources", "icon", "app.ico")
            self.iconbitmap(icon_path)
        except Exception as e:
            messagebox.showwarning("图标加载失败", f"错误原因: {str(e)}")

        self.file_path = tk.StringVar()
        self.save_path = tk.StringVar()
        self.true_min_date = datetime(2020,1,1)
        self.true_max_date = datetime.now()
        self.create_widgets()

    def process_data(self):
        """增强的数据处理逻辑（集成Google Sheet）"""
        if not self.file_path.get() or not self.save_path.get():
            messagebox.showwarning("输入错误", "请选择源文件和保存路径")
            return
        
        try:
            # 获取Google Sheet映射（提前获取避免重复调用）
            master_mapping = get_master_sku_mapping()
            
            raw_source_df = pd.read_csv(self.file_path.get(), delimiter='\t').iloc[1:]
            raw_source_df['posted-date'] = pd.to_datetime(raw_source_df['posted-date'], errors='coerce')
            raw_df = raw_source_df.dropna(subset=['posted-date'])
            
            start_date = datetime.strptime(self.start_cal.get_date(), "%Y-%m-%d")
            end_date = datetime.strptime(self.end_cal.get_date(), "%Y-%m-%d")
            
            with pd.ExcelWriter(self.save_path.get()) as writer:
                # 生成汇总表（保持不变）
                pivot_tables = generate_summary(raw_df, start_date, end_date)
                if pivot_tables:
                    start_row = 0
                    for month, pivot in pivot_tables:
                        pivot.to_excel(writer, sheet_name='Summary', index=False, startrow=start_row)
                        start_row += len(pivot) + 3
                
                # 分月处理逻辑
                if start_date.month != end_date.month or start_date.year != end_date.year:
                    monthly_data = split_data_by_month(raw_df, start_date, end_date)
                    for month_key, month_df in monthly_data.items():
                        month_start = month_df['posted-date'].min().to_pydatetime()
                        month_end = month_df['posted-date'].max().to_pydatetime()
                        
                        qty_df, _, _ = process_qty_data(month_df, month_start, month_end)
                        order_df = process_order_data(month_df)
                        
                        if qty_df is not None and order_df is not None:
                            merged_month = merge_order_qty(order_df, qty_df, raw_source_df)
                            if merged_month is not None:
                                # 添加master_sku列
                                merged_month['master_sku'] = merged_month['sku'].map(
                                    lambda x: master_mapping.get(str(x), ''))
                                merged_month.to_excel(
                                    writer,
                                    sheet_name=f"{month_key}_order_details",
                                    index=False
                                )
                else:
                    qty_df, _, _ = process_qty_data(self.file_path.get(), start_date, end_date)
                    order_df = process_order_data(raw_df)
                    
                    if qty_df is not None and order_df is not None:
                        merged_all = merge_order_qty(order_df, qty_df, raw_source_df)
                        if merged_all is not None:
                            # 添加master_sku列
                            merged_all['master_sku'] = merged_all['sku'].map(
                                lambda x: master_mapping.get(str(x), ''))
                            merged_all.to_excel(
                                writer,
                                sheet_name='order_details',
                                index=False
                            )

            messagebox.showinfo("处理完成", "报表生成成功！")
            
        except Exception as e:
            messagebox.showerror("处理错误", f"数据处理失败:\n{str(e)}")

    # 其余GUI代码保持不变（create_widgets、load_file等方法）

# 其余功能函数保持不变（fill_missing_qty、generate_summary等）

if __name__ == "__main__":
    app = AmazonProcessor()
    app.mainloop()