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



def get_resource_path(relative_path):
    """获取资源文件的路径，兼容开发模式和打包后的exe模式"""
    if getattr(sys, 'frozen', False):
        base_path = sys._MEIPASS  # PyInstaller 解压后的临时目录
    else:
        base_path = os.path.abspath(".")  # 当前脚本所在目录
    full_path = os.path.join(base_path, relative_path)
    print(f"[Debug] 计算出的路径: {full_path}")  # 打印出路径
    return full_path

def add_master_sku_from_gsheet(df):
    """从Google Sheet获取SKU映射并添加master_sku列"""
    try:
        credentials_path = get_resource_path("resources/auth/credentials.json")
        scope = ['https://spreadsheets.google.com/feeds',
                 'https://www.googleapis.com/auth/drive']
        
        # 认证并获取数据
        creds = ServiceAccountCredentials.from_json_keyfile_name(credentials_path, scope)
        client = gspread.authorize(creds)
        sheet = client.open("SKU Manual Mapping").sheet1
        
        # 创建SKU映射字典
        records = sheet.get_all_records()
        sku_mapping = {str(row['channel_sku']).strip(): str(row['sku_backup']).strip() 
                      for row in records if row['channel_sku']}
        
        # 添加新列
        df['master_sku'] = df['sku'].map(sku_mapping)
        return df
        
    except Exception as e:
        messagebox.showwarning("Google Sheet错误", 
            f"SKU匹配失败:\n{str(e)}\n"
            "请检查：\n"
            "1. 凭证文件位置是否正确\n"
            "2. 表格名称是否为'SKU Manual Mapping'\n"
            "3. 表格权限是否开放")
        return df



# 获取图标路径
icon_path = get_resource_path("resources/icon/app.ico")

# ================================ 修改后的QTY填充逻辑 ================================
def fill_missing_qty(merged_df, raw_source_df):
    """填充缺失的QTY值（新增sku匹配条件）"""
    try:
        # 仅处理QTY为空的情况
        mask = merged_df['QTY'].isna()
        if not mask.any():
            return merged_df
        
        # 获取需要填充的行（新增sku字段）
        fill_rows = merged_df[mask][['order-id', 'shipment-id', 'sku']].drop_duplicates()
        
        # 从原始数据中提取相关记录（新增sku匹配）
        source_data = raw_source_df[
            (raw_source_df['amount-type'] == 'ItemWithheldTax') &
            (raw_source_df['transaction-type'] == 'Order') &
            (raw_source_df['sku'].notna())  # 确保sku不为空
        ]
        
        # 计算补充数量（新增sku分组）
        qty_lookup = source_data.groupby(
            ['order-id', 'shipment-id', 'sku']  # 新增sku分组
        )['quantity-purchased'].sum().reset_index()
        qty_lookup.rename(columns={'quantity-purchased': '补充QTY'}, inplace=True)
        
        # 合并补充数据（新增sku匹配）
        filled_df = pd.merge(
            merged_df,
            qty_lookup,
            on=['order-id', 'shipment-id', 'sku'],  # 新增sku字段
            how='left'
        )
        
        # 填充逻辑保持不变
        filled_df['QTY'] = filled_df['QTY'].fillna(filled_df['补充QTY']).fillna(0)
        filled_df.drop(columns=['补充QTY'], inplace=True)
        
        print(f"[Debug] 已填充 {len(fill_rows)} 行的缺失QTY（使用sku匹配）")
        return filled_df
        
    except Exception as e:
        messagebox.showwarning("QTY填充错误", f"填充缺失数量失败:\n{str(e)}")
        return merged_df

# ================================ 合并逻辑 ================================
def merge_order_qty(order_df, qty_df, raw_source_df=None):
    """合并 Order 和 QTY 数据（三键 LEFT JOIN）"""
    try:
        # 定义合并键
        merge_keys = ['order-id', 'shipment-id', 'sku']
        
        # 检查必需列是否存在
        for df, name in [(order_df, 'Order'), (qty_df, 'QTY')]:
            missing = [col for col in merge_keys if col not in df.columns]
            if missing:
                raise ValueError(f"{name}表缺少关键列: {', '.join(missing)}")
        
        # 执行 LEFT JOIN
        merged_df = pd.merge(
            order_df,
            qty_df[merge_keys + ['quantity-purchased']],
            on=merge_keys,
            how='left',
            suffixes=('_order', '_qty')
        )
        
        # 重命名quantity列
        if 'quantity-purchased' in merged_df.columns:
            merged_df.rename(columns={'quantity-purchased': 'QTY'}, inplace=True)
        
        # 执行QTY填充
        if raw_source_df is not None:
            merged_df = fill_missing_qty(merged_df, raw_source_df)

        # 新增：添加master_sku列
        merged_df = add_master_sku_from_gsheet(merged_df)
        
        # 调整列顺序
        columns = ['master_sku'] + [col for col in merged_df.columns if col != 'master_sku']
        return merged_df[columns]

        return merged_df
        
    except Exception as e:
        messagebox.showerror("合并错误", f"合并Order/QTY失败:\n{str(e)}")
        return None

# ================================ 核心功能函数 ================================
def generate_summary(raw_df, start_date, end_date):
    """生成交易类型汇总表（保持原样）"""
    try:
        required_cols = ['transaction-type', 'amount-type', 'amount', 'posted-date']
        missing_cols = [col for col in required_cols if col not in raw_df.columns]
        if missing_cols:
            messagebox.showwarning("列缺失", f"缺少必要列: {', '.join(missing_cols)}")
            return None
        
        raw_df['posted-date'] = pd.to_datetime(raw_df['posted-date'], errors='coerce')
        raw_df = raw_df.dropna(subset=['posted-date'])
        
        mask = (raw_df['posted-date'] >= start_date) & (raw_df['posted-date'] <= end_date)
        df = raw_df[mask].copy()
        
        df['month'] = df['posted-date'].dt.to_period('M')
        months = df['month'].unique()
        
        pivot_tables = []
        for month in months:
            month_df = df[df['month'] == month]
            pivot = month_df.pivot_table(
                index=['amount-type'],
                columns=['transaction-type'],
                values='amount',
                aggfunc='sum',
                fill_value=0,
                margins=True,
                margins_name='Grand Total'
            )
            pivot_tables.append((month, pivot.round(2).reset_index()))
        
        return pivot_tables
        
    except Exception as e:
        messagebox.showerror("汇总错误", f"生成汇总表失败:\n{str(e)}")
        return None

def split_data_by_month(df, start_date, end_date):
    """智能分月处理函数"""
    monthly_data = {}
    current_date = start_date
    while current_date <= end_date:
        month_start = datetime(current_date.year, current_date.month, 1)
        month_end = (month_start + pd.DateOffset(months=1)) - pd.DateOffset(days=1)
        
        effective_start = max(current_date, month_start)
        effective_end = min(end_date, month_end)
        
        mask = (df['posted-date'] >= effective_start) & (df['posted-date'] <= effective_end)
        month_df = df[mask].copy()
        
        month_key = effective_start.strftime("%Y%m")
        monthly_data[month_key] = month_df
        
        current_date = effective_end + pd.DateOffset(days=1)
    
    return monthly_data

def process_qty_data(input_data, start_date, end_date):
    """增强版数量表处理"""
    try:
        if isinstance(input_data, str):
            df = pd.read_csv(input_data, delimiter='\t', encoding='utf-8')
            df = df.iloc[1:].reset_index(drop=True)
        else:
            df = input_data.copy()

        df['posted-date'] = pd.to_datetime(
            df['posted-date'], 
            format='%Y-%m-%d',
            errors='coerce'
        )
        df = df.dropna(subset=['posted-date'])
        
        mask = (df['posted-date'] >= start_date) & (df['posted-date'] <= end_date)
        df = df[mask]

        type_map = {
            "settlement-id": 'Int64', 
            "total-amount": 'float', 
            "amount": 'float',
            "order-item-code": 'Int64', 
            "quantity-purchased": 'Int64'
        }
        df = df.astype({k: v for k, v in type_map.items() if k in df.columns})

        df = df[
            (df['transaction-type'] == 'Order') & 
            (df['marketplace-name'] == 'Amazon.com')
        ].drop(columns=[
            "settlement-id", "settlement-start-date", "settlement-end-date",
            "deposit-date", "total-amount", "currency", "transaction-type",
            "merchant-order-id", "adjustment-id", "marketplace-name",
            "fulfillment-id", "posted-date", "posted-date-time",
            "order-item-code", "merchant-order-item-id",
            "merchant-adjustment-item-id", "promotion-id"
        ])

        df['des-type'] = df['amount-description'] + ":" + df['amount-type']
        df = df[df['des-type'] == "Principal:ItemPrice"]
        return df.groupby(
            ["order-id", "shipment-id", "sku"], 
            as_index=False
        )["quantity-purchased"].sum().sort_values("shipment-id"), start_date, end_date

    except Exception as e:
        messagebox.showerror("处理错误", f"数量表处理失败:\n{str(e)}")
        return None, None, None

def process_order_data(raw_df):
    """订单表处理（保持原样）"""
    try:
        df = raw_df.copy()
        df = df[
            (df['transaction-type'] == 'Order') &
            (df['amount-type'].isin(['ItemPrice', 'ItemWithheldTax', 'Promotion'])) &
            (df['marketplace-name'] == 'Amazon.com')
        ]
        
        cols_to_drop = [
            'settlement-id', 'settlement-start-date', 'settlement-end-date',
            'deposit-date', 'total-amount', 'currency', 'transaction-type',
            'merchant-order-id', 'adjustment-id', 'marketplace-name',
            'fulfillment-id', 'posted-date', 'posted-date-time',
            'order-item-code', 'merchant-order-item-id',
            'merchant-adjustment-item-id', 'quantity-purchased', 'promotion-id'
        ]
        df = df.drop(columns=[c for c in cols_to_drop if c in df.columns])
        
        df['des-type'] = df['amount-description'] + ":" + df['amount-type']
        pivot_df = df.pivot_table(
            index=['order-id', 'shipment-id', 'sku'],
            columns='des-type',
            values='amount',
            aggfunc='sum',
            fill_value=0
        ).reset_index()

        required_columns = [
            "Principal:ItemPrice", "Principal:Promotion",
            "Tax:ItemPrice", "MarketplaceFacilitatorTax-Principal:ItemWithheldTax",
            "MarketplaceFacilitatorVAT-Principal:ItemWithheldTax",
            "LowValueGoodsTax-Principal:ItemWithheldTax",
            "Shipping:ItemPrice", "Shipping:Promotion",
            "GiftWrap:ItemPrice", "GiftWrap:Promotion",
            "GiftWrapTax:ItemPrice", "MarketplaceFacilitatorTax-Other:ItemWithheldTax"
        ]

        existing_columns = pivot_df.columns.tolist()
        for col in required_columns:
            if col not in existing_columns:
                pivot_df[col] = 0

        pivot_df['Product Amount'] = pivot_df['Principal:ItemPrice'] + pivot_df['Principal:Promotion']
        pivot_df = pivot_df.drop(['Principal:ItemPrice', 'Principal:Promotion'], axis=1, errors='ignore')

        product_tax_cols = [
            'Tax:ItemPrice',
            'MarketplaceFacilitatorTax-Principal:ItemWithheldTax',
            'MarketplaceFacilitatorVAT-Principal:ItemWithheldTax',
            'LowValueGoodsTax-Principal:ItemWithheldTax'
        ]
        pivot_df['Product Tax'] = pivot_df[product_tax_cols].sum(axis=1)
        pivot_df = pivot_df.drop(product_tax_cols, axis=1, errors='ignore')

        pivot_df['Shipping'] = pivot_df['Shipping:ItemPrice'] + pivot_df['Shipping:Promotion']
        pivot_df = pivot_df.drop(['Shipping:ItemPrice', 'Shipping:Promotion'], axis=1, errors='ignore')

        pivot_df['Giftwrap'] = pivot_df['GiftWrap:ItemPrice'] + pivot_df['GiftWrap:Promotion']
        pivot_df = pivot_df.drop(['GiftWrap:ItemPrice', 'GiftWrap:Promotion'], axis=1, errors='ignore')

        giftwrap_tax_cols = [
            'GiftWrapTax:ItemPrice',
            'MarketplaceFacilitatorTax-Other:ItemWithheldTax'
        ]
        pivot_df['Giftwrap Tax'] = pivot_df[giftwrap_tax_cols].sum(axis=1)
        pivot_df = pivot_df.drop(giftwrap_tax_cols, axis=1, errors='ignore')

        pivot_df['Total_amount'] = pivot_df[['Product Tax', 'Product Amount', 'Giftwrap', 'Giftwrap Tax']].sum(axis=1)
        
        if 'Shipping Tax' not in pivot_df.columns:
            pivot_df['Shipping Tax'] = 0
        pivot_df['Total_shipping'] = pivot_df['Shipping'] + pivot_df['Shipping Tax']

        pivot_df['tax_rate'] = np.where(
            pivot_df['Product Amount'] != 0,
            (pivot_df['Product Tax'] / pivot_df['Product Amount']).round(2),
            0
        )
        pivot_df['tax_rate'] = pivot_df['tax_rate'].apply(lambda x: f"{x:.0%}")

        final_columns = [
            'order-id', 'shipment-id', 'sku',
            'Product Amount', 'Product Tax', 'tax_rate',
            'Shipping', 'Shipping Tax', 'Total_shipping',
            'Giftwrap', 'Giftwrap Tax', 'Total_amount'
        ]
        
        return pivot_df[final_columns].sort_values("shipment-id")

    except Exception as e:
        messagebox.showerror("处理错误", f"订单表处理失败:\n{str(e)}")
        return None

# ================================ GUI界面类 ================================
class AmazonProcessor(tk.Tk):
    def __init__(self):
        super().__init__()

        try:
            if getattr(sys, 'frozen', False):
                # 打包后的路径：sys._MEIPASS 指向临时资源目录
                base_path = sys._MEIPASS
                print("[Debug] 运行模式: 打包模式")
            else:
                # 开发模式：从 src 目录向上返回一级到项目根目录
                current_dir = os.path.dirname(os.path.abspath(__file__))  # src 目录
                base_path = os.path.dirname(current_dir)                   # 项目根目录
                print("[Debug] 运行模式: 开发模式")

            # 计算图标路径
            icon_path = os.path.join(base_path, "resources", "icon", "app.ico")
            icon_path = os.path.normpath(icon_path)

            # 调试输出
            print(f"[Debug] 项目根目录: {base_path}")
            print(f"[Debug] 图标路径: {icon_path}")
            print(f"[Debug] 文件是否存在: {os.path.exists(icon_path)}")

            # 加载图标
            self.iconbitmap(icon_path)

        except Exception as e:
            messagebox.showwarning(
                "图标加载失败",
                f"错误原因: {str(e)}\n"
                f"base_path: {base_path}\n"
                f"icon_path: {icon_path}"
            )

        self.title("US Amazon Processor v3.1")
        self.geometry("550x470")
        self.configure(bg="#f0f0f0")
        self.file_path = tk.StringVar()
        self.save_path = tk.StringVar()
        self.true_min_date = datetime(2020,1,1)
        self.true_max_date = datetime.now()
        self.create_widgets()
        
    def create_widgets(self):
        """Create UI components"""
        file_frame = tk.LabelFrame(
            self, 
            text="Input",
            font=('微软雅黑',10),
            bg="#f0f0f0",
            padx=10,
            pady=5
        )
        file_frame.pack(pady=10, padx=15, fill="x")
        tk.Label(file_frame, text="Input Path:", bg="#f0f0f0").grid(row=0, column=0)
        tk.Entry(file_frame, textvariable=self.file_path, width=55).grid(row=0, column=1)
        tk.Button(file_frame, text="Browse", command=self.load_file, width=10).grid(row=0, column=2)
        
        save_frame = tk.LabelFrame(
            self, 
            text="Output",
            font=('微软雅黑',10),
            bg="#f0f0f0",
            padx=10,
            pady=5
        )
        save_frame.pack(pady=10, padx=15, fill="x")
        tk.Label(save_frame, text="Output Path:", bg="#f0f0f0").grid(row=0, column=0)
        tk.Entry(save_frame, textvariable=self.save_path, width=55).grid(row=0, column=1)
        tk.Button(save_frame, text="Browse", command=self.save_file, width=10).grid(row=0, column=2)
        
        date_frame = tk.LabelFrame(
            self, 
            text="Date Range",
            font=('微软雅黑',10),
            bg="#f0f0f0",
            padx=10,
            pady=5
        )
        date_frame.pack(pady=10, padx=15, fill="x")
        self.start_cal = Calendar(
            date_frame, 
            date_pattern="y-mm-dd",
            mindate=datetime(2020,1,1),
            maxdate=datetime(2100,12,31)
        )
        self.end_cal = Calendar(
            date_frame, 
            date_pattern="y-mm-dd",
            mindate=datetime(2020,1,1),
            maxdate=datetime(2100,12,31)
        )
        self.start_cal.grid(row=1, column=0, padx=10)
        self.end_cal.grid(row=1, column=1, padx=10)
        
        tk.Button(self, text="Submit", command=self.process_data,
                 font=('Arial',12), bg="#2196F3", fg="white",
                 width=20).pack(pady=20)

    def process_data(self):
        """Enhanced data processing logic with merging"""
        if not self.file_path.get() or not self.save_path.get():
            messagebox.showwarning("Input Error", "Please select source file and save path")
            return
        
        try:
            # 读取原始数据副本用于QTY填充
            raw_source_df = pd.read_csv(self.file_path.get(), delimiter='\t').iloc[1:]
            raw_source_df['posted-date'] = pd.to_datetime(raw_source_df['posted-date'], errors='coerce')
            
            # 保持原有处理流程
            raw_df = raw_source_df.copy()
            raw_df = raw_df.dropna(subset=['posted-date'])
            
            start_date = datetime.strptime(self.start_cal.get_date(), "%Y-%m-%d")
            end_date = datetime.strptime(self.end_cal.get_date(), "%Y-%m-%d")
            
            with pd.ExcelWriter(self.save_path.get()) as writer:
                # Generate summary tables
                pivot_tables = generate_summary(raw_df, start_date, end_date)
                if pivot_tables:
                    start_row = 0
                    for month, pivot in pivot_tables:
                        pivot.to_excel(
                            writer,
                            sheet_name='Summary',
                            index=False,
                            startrow=start_row,
                            float_format="%.2f"
                        )
                        start_row += len(pivot) + 3
                
                # 初始化合并结果存储
                all_merged = []
                
                # Monthly processing logic
                if start_date.month != end_date.month or start_date.year != end_date.year:
                    monthly_data = split_data_by_month(raw_df, start_date, end_date)
                    for month_key, month_df in monthly_data.items():
                        month_start = month_df['posted-date'].min().to_pydatetime()
                        month_end = month_df['posted-date'].max().to_pydatetime()
                        
                        # 处理QTY和Order数据
                        qty_df, _, _ = process_qty_data(month_df, month_start, month_end)
                        order_df = process_order_data(month_df)
                        
                        # 写入原有sheet
                        qty_df.to_excel(writer, sheet_name=f"{month_key}_qty", index=False)
                        order_df.to_excel(writer, sheet_name=f"{month_key}_order", index=False)
                        
                        # 执行分月合并
                        if qty_df is not None and order_df is not None:
                            merged_month = merge_order_qty(order_df, qty_df, raw_source_df)
                            if merged_month is not None:
                                merged_month.to_excel(
                                    writer,
                                    sheet_name=f"{month_key}_order_details",
                                    index=False
                                )
                                all_merged.append(merged_month)
                
                else:
                    # 处理非分月情况
                    qty_df, _, _ = process_qty_data(self.file_path.get(), start_date, end_date)
                    order_df = process_order_data(raw_df)
                    
                    # 写入原有sheet
                    qty_df.to_excel(writer, sheet_name='qty', index=False)
                    order_df.to_excel(writer, sheet_name='order', index=False)
                    
                    # 执行整体合并
                    if qty_df is not None and order_df is not None:
                        merged_all = merge_order_qty(order_df, qty_df, raw_source_df)
                        if merged_all is not None:
                            merged_all.to_excel(
                                writer,
                                sheet_name='order_details',
                                index=False
                            )
                            all_merged.append(merged_all)

            messagebox.showinfo(
                "Processing Complete",
                f"Report generated successfully!\nDate range: {start_date.date()} to {end_date.date()}"
            )
            
        except Exception as e:
            messagebox.showerror("Processing Error", f"Data processing failed:\n{str(e)}")

    def calculate_amount_sum(self, file_path):
        try:
            df = pd.read_csv(file_path, delimiter='\t')
            return df['amount'].sum() if 'amount' in df.columns else None
        except Exception as e:
            messagebox.showerror("Error", f"Failed to calculate amount:\n{str(e)}")
            return None

    def load_file(self):
        path = filedialog.askopenfilename(filetypes=[("Text Files", "*.txt")])
        if not path: return
        self.file_path.set(path)
        
        try:
            total_amount = self.calculate_amount_sum(path)
            if total_amount and not messagebox.askyesno("Confirmation", 
                f"Total amount: {total_amount:.2f}\nContinue processing?"):
                return

            df = pd.read_csv(path, delimiter='\t', usecols=['posted-date'], dtype={'posted-date': 'string'})
            dates = pd.to_datetime(df['posted-date'], format='%Y-%m-%d', errors='coerce').dropna()
            
            if dates.empty:
                messagebox.showwarning("Warning", "No valid date data found")
                return
                
            self.true_min_date = dates.min().to_pydatetime()
            self.true_max_date = dates.max().to_pydatetime()
            
            self.start_cal.selection_set(self.true_min_date)
            self.end_cal.selection_set(self.true_max_date)
            self.start_cal.config(mindate=self.true_min_date, maxdate=self.true_max_date)
            self.end_cal.config(mindate=self.true_min_date, maxdate=self.true_max_date)
            
        except Exception as e:
            messagebox.showerror("Error", f"File loading failed:\n{str(e)}")

    def save_file(self):
        path = filedialog.asksaveasfilename(
            defaultextension=".xlsx",
            filetypes=[("Excel Files", "*.xlsx")]
        )
        if path:
            self.save_path.set(path)

if __name__ == "__main__":
    app = AmazonProcessor()
    app.mainloop()