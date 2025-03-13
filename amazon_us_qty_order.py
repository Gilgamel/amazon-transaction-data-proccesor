import tkinter as tk
from tkinter import filedialog, messagebox
from tkcalendar import Calendar
import pandas as pd
from datetime import datetime
import numpy as np

# ================================ 数据处理函数 ================================
def process_qty_data(file_path, start_date, end_date):
    """处理数量表(qty)的完整逻辑"""
    try:
        # 读取完整数据
        df = pd.read_csv(file_path, delimiter='\t', encoding='utf-8')
        df = df.iloc[1:].reset_index(drop=True)  # 跳过首行

        # 精确日期处理
        df['posted-date'] = pd.to_datetime(
            df['posted-date'], 
            format='%Y-%m-%d',  # 明确日期格式
            errors='coerce'
        )
        df = df.dropna(subset=['posted-date'])
        
        # 获取准确日期范围
        min_date = df['posted-date'].min().to_pydatetime()
        max_date = df['posted-date'].max().to_pydatetime()
        
        # 应用日期筛选
        mask = (df['posted-date'] >= start_date) & (df['posted-date'] <= end_date)
        df = df[mask]

        # 列类型转换（完整原始映射）
        type_map = {
            "settlement-id": 'Int64', 
            "total-amount": 'float', 
            "amount": 'float',
            "order-item-code": 'Int64', 
            "quantity-purchased": 'Int64'
        }
        df = df.astype({k: v for k, v in type_map.items() if k in df.columns})

        # 数据清洗流程
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

        # 分组聚合
        df['des-type'] = df['amount-description'] + ":" + df['amount-type']
        df = df[df['des-type'] == "Principal:ItemPrice"]
        return df.groupby(
            ["order-id", "shipment-id", "sku"], 
            as_index=False
        )["quantity-purchased"].sum().sort_values("shipment-id"), min_date, max_date

    except Exception as e:
        messagebox.showerror("处理错误", f"数量表处理失败:\n{str(e)}")
        return None, None, None

def process_order_data(raw_df):
    """处理订单表(order)的增强逻辑"""
    try:
        df = raw_df.copy()
        
        # Power Query等效步骤
        # 筛选条件
        df = df[
            (df['transaction-type'] == 'Order') &
            (df['amount-type'].isin(['ItemPrice', 'ItemWithheldTax', 'Promotion'])) &
            (df['marketplace-name'] == 'Amazon.com')
        ]
        
        # 列处理
        cols_to_drop = [
            'settlement-id', 'settlement-start-date', 'settlement-end-date',
            'deposit-date', 'total-amount', 'currency', 'transaction-type',
            'merchant-order-id', 'adjustment-id', 'marketplace-name',
            'fulfillment-id', 'posted-date', 'posted-date-time',
            'order-item-code', 'merchant-order-item-id',
            'merchant-adjustment-item-id', 'quantity-purchased', 'promotion-id'
        ]
        df = df.drop(columns=[c for c in cols_to_drop if c in df.columns])
        
        # 数据透视
        df['des-type'] = df['amount-description'] + ":" + df['amount-type']
        pivot_df = df.pivot_table(
            index=['order-id', 'shipment-id', 'sku'],
            columns='des-type',
            values='amount',
            aggfunc='sum',
            fill_value=0
        ).reset_index()
        
        # 动态列处理
        required_columns = [
            "Principal:ItemPrice", "Tax:ItemPrice", "Shipping:ItemPrice",
            "MarketplaceFacilitatorTax-Principal:ItemWithheldTax",
            "ShippingTax:ItemPrice", "MarketplaceFacilitatorTax-Shipping:ItemWithheldTax",
            # ...（其他列保持不变）
        ]
        for col in required_columns:
            if col not in pivot_df.columns:
                pivot_df[col] = 0
                
        # 税款计算
        tax_cols = [c for c in pivot_df.columns if "Tax" in c and "Withheld" in c]
        pivot_df['Shipping Tax'] = pivot_df[tax_cols].sum(axis=1)
        
        return pivot_df.sort_values("shipment-id")

    except Exception as e:
        messagebox.showerror("处理错误", f"订单表处理失败:\n{str(e)}")
        return None

# ================================ GUI界面类 ================================
class AmazonProcessor(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("亚马逊数据处理器 v3.0")
        self.geometry("900x800")
        self.configure(bg="#f0f0f0")
        
        # 初始化变量
        self.file_path = tk.StringVar()
        self.save_path = tk.StringVar()
        self.true_min_date = datetime(2020,1,1)
        self.true_max_date = datetime.now()
        
        # 创建控件
        self.create_widgets()
        
    def create_widgets(self):
        """创建界面组件"""
        # 文件选择区域
        file_frame = self.create_label_frame(" 文件选择 ", 0)
        tk.Label(file_frame, text="源文件:", bg="#f0f0f0").grid(row=0, column=0)
        tk.Entry(file_frame, textvariable=self.file_path, width=55).grid(row=0, column=1)
        tk.Button(file_frame, text="浏览", command=self.load_file, width=10).grid(row=0, column=2)
        
        # 保存路径区域
        save_frame = self.create_label_frame(" 输出设置 ", 1)
        tk.Label(save_frame, text="保存路径:", bg="#f0f0f0").grid(row=0, column=0)
        tk.Entry(save_frame, textvariable=self.save_path, width=55).grid(row=0, column=1)
        tk.Button(save_frame, text="浏览", command=self.save_file, width=10).grid(row=0, column=2)
        
        # 日期选择区域
        date_frame = self.create_label_frame(" 日期范围 ", 2)
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
        
        # 处理按钮
        tk.Button(self, text="开始处理", command=self.process_data,
                 font=('Arial',12), bg="#2196F3", fg="white",
                 width=20).pack(pady=20)
    
    def create_label_frame(self, text, pady_pos):
        """创建统一风格的LabelFrame"""
        frame = tk.LabelFrame(
            self, 
            text=text,
            font=('微软雅黑',10),
            bg="#f0f0f0",
            padx=10,
            pady=5
        )
        frame.pack(pady=10, padx=15, fill="x")
        return frame
    
    def load_file(self):
        """加载文件并精确更新日期范围"""
        path = filedialog.askopenfilename(filetypes=[("Text Files", "*.txt")])
        if not path: return
        
        self.file_path.set(path)
        try:
            # 优化读取：只读取日期列
            df = pd.read_csv(
                path,
                delimiter='\t',
                usecols=['posted-date'],
                dtype={'posted-date': 'string'},
                na_filter=False
            )
            
            # 转换日期
            dates = pd.to_datetime(
                df['posted-date'],
                format='%Y-%m-%d',
                errors='coerce'
            )
            valid_dates = dates.dropna()
            
            if valid_dates.empty:
                messagebox.showwarning("警告", "未找到有效日期数据")
                return
                
            # 获取真实日期范围
            self.true_min_date = valid_dates.min().to_pydatetime()
            self.true_max_date = valid_dates.max().to_pydatetime()
            
            # 更新日期控件
            self.start_cal.selection_set(self.true_min_date)
            self.end_cal.selection_set(self.true_max_date)
            self.start_cal.config(
                mindate=self.true_min_date,
                maxdate=self.true_max_date
            )
            self.end_cal.config(
                mindate=self.true_min_date,
                maxdate=self.true_max_date
            )
            
        except Exception as e:
            messagebox.showerror("错误", f"日期解析失败:\n{str(e)}")
    
    def save_file(self):
        """处理保存路径"""
        path = filedialog.asksaveasfilename(
            defaultextension=".xlsx",
            filetypes=[("Excel Files", "*.xlsx")]
        )
        if path:
            self.save_path.set(path)
    
    def process_data(self):
        """执行数据处理"""
        if not self.file_path.get():
            messagebox.showwarning("输入错误", "请选择源文件")
            return
        if not self.save_path.get():
            messagebox.showwarning("输入错误", "请设置保存路径")
            return
        
        try:
            # 获取日期参数
            start_date = self.start_cal.get_date()
            end_date = self.end_cal.get_date()
            
            # 处理数量表
            qty_df, file_min, file_max = process_qty_data(
                self.file_path.get(),
                datetime.strptime(start_date, "%Y-%m-%d"),
                datetime.strptime(end_date, "%Y-%m-%d")
            )
            
            # 处理订单表
            raw_df = pd.read_csv(self.file_path.get(), delimiter='\t').iloc[1:]
            order_df = process_order_data(raw_df)
            
            # 保存结果
            with pd.ExcelWriter(self.save_path.get()) as writer:
                qty_df.to_excel(writer, sheet_name='qty', index=False)
                order_df.to_excel(writer, sheet_name='order', index=False)
                
            messagebox.showinfo(
                "处理完成",
                f"成功生成报表！\n"
                f"文件日期范围: {file_min.date()} 至 {file_max.date()}\n"
                f"筛选日期范围: {start_date} 至 {end_date}"
            )
            
        except Exception as e:
            messagebox.showerror("处理错误", f"数据处理失败:\n{str(e)}")

# ================================ 主程序入口 ================================
if __name__ == "__main__":
    app = AmazonProcessor()
    app.mainloop()