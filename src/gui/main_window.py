import tkinter as tk
from tkinter import filedialog, messagebox
from tkcalendar import Calendar
import pandas as pd
import numpy as np
import os
import sys
from datetime import datetime

from processor.data_processing import (
    generate_summary,
    split_data_by_month,
    process_qty_data,
    process_order_data,
    merge_order_qty
)
from utils.file_utils import get_resource_path
from utils.auth_utils import load_environment
from processor.google_sheets import load_gsheet_data

class AmazonProcessor(tk.Tk):
    def __init__(self):
        super().__init__()
        load_environment()  # 加载环境变量
        
        try:
            icon_path = get_resource_path("resources/icon/app.ico")
            self.iconbitmap(icon_path)
        except Exception as e:
            messagebox.showwarning("图标加载失败", f"错误原因: {str(e)}")

        self.title("US Amazon Processor v3.1")
        self.geometry("600x470")
        self.configure(bg="#f0f0f0")
        self.file_path = tk.StringVar()
        self.save_path = tk.StringVar()
        self.true_min_date = datetime(2020,1,1)
        self.true_max_date = datetime.now()
        self.create_widgets()
        
        # 首次运行时检查认证状态
        self.check_auth_status()
    
    def check_auth_status(self):
        """首次运行时检查Google认证状态"""
        app_data_dir = os.path.join(os.path.expanduser("~"), ".amazon-processor")
        token_path = os.path.join(app_data_dir, "token.pickle")
        
        if not os.path.exists(token_path):
            response = messagebox.askyesno(
                "First-time Authorization",
                "This application requires Google Account authorization to access Google Sheets.\nProceed now?",
                icon='question'
            )
            if response:
                try:
                    # 从auth_utils导入get_google_creds
                    from utils.auth_utils import get_google_creds
                    get_google_creds()  
                    messagebox.showinfo("Authorization Successful", "All features are now available!")
                except Exception as e:
                    messagebox.showerror(
                        "Authorization Failed",
                        f"Authorization could not be completed: {str(e)}\nPlease check your internet connection and try again."
                    )
                    self.destroy()  # 关闭应用
            else:
                messagebox.showwarning(
                    "Authorization Required",
                    "You must complete authorization to use core features.\nThe application will now exit."
                )
                self.destroy()

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
            

            # ========== 加载成本表 ==========
            print("\n[步骤1/4] 开始加载成本数据...")
            landed_cost_data = load_gsheet_data("landed_cost")
            pdb_us_data = load_gsheet_data("pdb_us")
        
            # 检查数据完整性
            if not landed_cost_data or not pdb_us_data:
                messagebox.showerror(
                    "数据缺失", 
                    "无法加载成本表，请检查控制台错误信息"
                )
                return
            print("✅ 成本数据加载完成")

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

                                if not merged_month.empty:
                                    required_cols = ['master_sku', 'QTY', 'Total_amount']
                                    if all(col in merged_month.columns for col in required_cols):
                                        grouped = merged_month.groupby('master_sku', as_index=False).agg({
                                            'QTY': 'sum',                
                                            'Total_amount': 'sum'        
                                        }).rename(columns={
                                            'QTY': 'total QTY',          
                                            'Total_amount': 'total amount'
                                        })

                                        if not grouped.empty:
                                            # 处理除零错误
                                            grouped['product_rate'] = np.where(
                                                grouped['total QTY'] > 0,
                                                (grouped['total amount'] / grouped['total QTY']).round(2),
                                                0.0  # QTY为0时设为0
                                            )
                                            
                                        # 修复的lambda表达式 - 使用正确的三元运算符语法
                                        grouped['product_cost'] = grouped['master_sku'].apply(
                                            lambda sku: (0.0 if str(sku).strip().lower() == "shipping" 
                                                        else landed_cost_data.get(
                                                            str(sku).strip(), 
                                                            pdb_us_data.get(str(sku).strip(), None))
                                        )

                                        # 计算total_cost
                                        grouped['total_cost'] = grouped['product_cost'] * grouped['total QTY']

                                        # 添加Shipping汇总行
                                        try:
                                            sum_total_shipping = merged_month['Total_shipping'].sum()
        
                                            if sum_total_shipping != 0:
                                                new_row = pd.DataFrame([{
                                                    'master_sku': 'Shipping',
                                                    'total QTY': 1,
                                                    'total amount': sum_total_shipping,
                                                    'product_rate': sum_total_shipping,
                                                    'product_cost': 0,
                                                    'total_cost': 0

                                            }])
            
                                            # 合并新行
                                            grouped = pd.concat([grouped, new_row], ignore_index=True)
            
                                        except KeyError as e:
                                            print(f"[Warning] {month_key}_order_details 缺少Total_shipping列: {str(e)}")
                                        except Exception as e:
                                            print(f"[Error] 添加Shipping行失败: {str(e)}")

                                        grouped.to_excel(
                                            writer,
                                            sheet_name=f"{month_key}_order_import",
                                            index=False
                                        )
                    

                    
                                    else:
                                        print(f"[Warning] {month_key}_order_details 缺少必要列")

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

                            if not merged_all.empty:
                                required_cols = ['master_sku', 'QTY', 'Total_amount']
                                if all(col in merged_all.columns for col in required_cols):
                                    grouped = merged_all.groupby('master_sku', as_index=False).agg({
                                        'QTY': 'sum',               
                                        'Total_amount': 'sum'        
                                    }).rename(columns={
                                        'QTY': 'total QTY',
                                        'Total_amount': 'total amount'
                                    })

                                    try:
                                        if 'total QTY' in grouped.columns and 'total amount' in grouped.columns:
                                            grouped['product_rate'] = grouped['total amount'] / grouped['total QTY']
                                            # 处理无效值
                                            grouped['product_rate'] = grouped['product_rate'].replace([np.inf, -np.inf], 0).fillna(0).round(2)
                                    except Exception as e:
                                        print(f"[Error] 计算product_rate失败: {str(e)}")

                                    # 修复的lambda表达式 - 使用正确的三元运算符语法
                                    grouped['product_cost'] = grouped['master_sku'].apply(
                                        lambda sku: (0.0 if str(sku).strip().lower() == "shipping" 
                                                    else landed_cost_data.get(
                                                        str(sku).strip(), 
                                                        pdb_us_data.get(str(sku).strip(), None)))
                                    )

                                    # 计算total_cost
                                    grouped['total_cost'] = grouped['product_cost'] * grouped['total QTY']
                                    

                                    # 添加Shipping行逻辑
                                    try:
                                        # 添加Shipping行逻辑    
                                        sum_total_shipping = merged_all['Total_shipping'].sum()
        
                                        if sum_total_shipping != 0:
                                            new_row = pd.DataFrame([{
                                                'master_sku': 'Shipping',
                                                'total QTY': 1,
                                                'total amount': sum_total_shipping,
                                                'product_rate': sum_total_shipping,
                                                'product_cost': 0,
                                                'total_cost': 0
                                            }])
            
                                            # 确保列顺序匹配
                                            new_row = new_row[grouped.columns]
                                            grouped = pd.concat([grouped, new_row], ignore_index=True)
            
                                    except KeyError as e:
                                        print(f"[Warning] order_details 缺少Total_shipping列: {str(e)}")
                                    except Exception as e:
                                        print(f"[Error] 添加Shipping行失败: {str(e)}")

                                    # 列顺序调整
                                    final_columns = [
                                        'master_sku', 
                                        'total QTY', 
                                        'total amount', 
                                        'product_rate',
                                        'product_cost', 
                                        'total_cost'
                                    ]
                                    grouped = grouped[final_columns]

                                    grouped.to_excel(
                                        writer,
                                        sheet_name='order_import',
                                        index=False
                                    )
                                else:
                                    print("[Warning] order_details 缺少必要列")


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
        
            # 先配置日期范围限制
            self.start_cal.config(mindate=self.true_min_date, maxdate=self.true_max_date)
            self.end_cal.config(mindate=self.true_min_date, maxdate=self.true_max_date)
        
            # 再设置选中日期
            self.start_cal.selection_set(self.true_min_date)
            self.end_cal.selection_set(self.true_max_date)
        
            # 强制刷新控件
            self.start_cal.update()
            self.end_cal.update()

        
        except Exception as e:
            messagebox.showerror("Error", f"File loading failed:\n{str(e)}")

    def save_file(self):
        path = filedialog.asksaveasfilename(
            defaultextension=".xlsx",
            filetypes=[("Excel Files", "*.xlsx")]
        )
        if path:
            self.save_path.set(path)