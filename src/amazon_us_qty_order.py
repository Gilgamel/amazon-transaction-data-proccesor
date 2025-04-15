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
from google_auth_oauthlib.flow import InstalledAppFlow
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request  # <--- 关键修复
import pickle
import webbrowser
from dotenv import load_dotenv



# 加载环境变量（开发环境）
def load_environment():
    """安全加载环境配置"""
    try:
        # 优先从项目根目录加载.env文件
        env_path = os.path.join(os.path.dirname(__file__), '..', '.env')
        if os.path.exists(env_path):
            load_dotenv(dotenv_path=env_path)
        else:
            # 打包后从可执行文件同级目录加载
            load_dotenv(dotenv_path=os.path.join(sys._MEIPASS, '.env'))
    except Exception as e:
        print(f"[环境加载警告] {str(e)}")


# 初始化环境配置
load_environment()

def get_google_creds():
    """安全获取Google API凭据（优化存储路径版）"""
    SCOPES = [
        'https://www.googleapis.com/auth/spreadsheets',
        'https://www.googleapis.com/auth/drive.readonly'
    ]
    
    # 创建应用专属用户数据目录
    app_data_dir = os.path.join(os.path.expanduser("~"), ".amazon-processor")
    os.makedirs(app_data_dir, exist_ok=True)
    token_file = os.path.join(app_data_dir, "token.pickle")

    try:
        # 验证环境变量
        required_envs = ['GOOGLE_CLIENT_ID', 'GOOGLE_CLIENT_SECRET']
        missing = [var for var in required_envs if not os.getenv(var)]
        if missing:
            raise ValueError(f"缺少环境变量: {', '.join(missing)}")

        # 动态构建客户端配置
        client_config = {
            "installed": {
                "client_id": os.getenv('GOOGLE_CLIENT_ID'),
                "client_secret": os.getenv('GOOGLE_CLIENT_SECRET'),
                "auth_uri": "https://accounts.google.com/o/oauth2/auth",
                "token_uri": "https://oauth2.googleapis.com/token",
                "redirect_uris": ["http://localhost:8080"],
                "project_id": os.getenv('GOOGLE_PROJECT_ID', 'amazon-processor')
            }
        }

        creds = None
        # 尝试加载已有凭据
        if os.path.exists(token_file):
            try:
                with open(token_file, 'rb') as token:
                    creds = pickle.load(token)
            except (EOFError, pickle.UnpicklingError) as e:
                print(f"[警告] 凭证文件损坏，将重新授权: {str(e)}")
                os.remove(token_file)

        # 凭据管理流程
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                try:
                    creds.refresh(Request())
                except Exception as refresh_error:
                    print(f"[认证刷新失败] {str(refresh_error)}")
                    creds = None

            if not creds:
                # 启动全新授权流程
                flow = InstalledAppFlow.from_client_config(client_config, SCOPES)
                webbrowser.open(flow.authorization_url()[0])
                creds = flow.run_local_server(port=8080)

            # 保存新凭据
            with open(token_file, 'wb') as token:
                pickle.dump(creds, token)
                print(f"[凭证存储] 已保存到: {token_file}")

        return creds

    except Exception as e:
        error_msg = f"认证失败: {str(e)}\n建议操作:\n"
        error_msg += "1. 检查网络连接\n2. 确认客户端ID/密钥正确\n3. 重新尝试授权"
        messagebox.showerror("认证错误", error_msg)
        raise  # 向上传递异常以中断流程


# ================== Google Sheet集成部分 ==================
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


# ================== 新增函数：加载Google Sheet数据 ==================
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

# ================== 修改后的合并函数 ==================
def merge_order_qty(order_df, qty_df, raw_source_df=None):
    """合并 Order 和 QTY 数据（新增master_sku列）"""
    try:
        merge_keys = ['order-id', 'shipment-id', 'sku']
        
        # 数据验证
        for df, name in [(order_df, 'Order'), (qty_df, 'QTY')]:
            missing = [col for col in merge_keys if col not in df.columns]
            if missing:
                raise ValueError(f"{name}表缺少关键列: {', '.join(missing)}")
        
        # 合并数据
        merged_df = pd.merge(
            order_df,
            qty_df[merge_keys + ['quantity-purchased']],
            on=merge_keys,
            how='left'
        )
        
        # 列重命名
        if 'quantity-purchased' in merged_df.columns:
            merged_df.rename(columns={'quantity-purchased': 'QTY'}, inplace=True)
        
        # 数量填充
        if raw_source_df is not None:
            merged_df = fill_missing_qty(merged_df, raw_source_df)
        
        # 添加master_sku列
        merged_df = add_master_sku_from_gsheet(merged_df)
        
        # 列顺序调整（确保master_sku在第一列）
        columns = [col for col in merged_df.columns if col != 'master_sku'] + ['master_sku']
        print(f"[Debug] 最终列顺序：{columns}")
        
        return merged_df[columns]
        
    except Exception as e:
        messagebox.showerror("合并错误", f"数据处理失败：\n{str(e)}")
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

        # ====== 新增代码 ======
        # 检查用户认证状态（首次运行检测）
        self.check_auth_status()  # <--- 新增调用

        self.title("US Amazon Processor v3.1")
        self.geometry("600x470")
        self.configure(bg="#f0f0f0")
        self.file_path = tk.StringVar()
        self.save_path = tk.StringVar()
        self.true_min_date = datetime(2020,1,1)
        self.true_max_date = datetime.now()
        self.create_widgets()
        
        # ====== 新增方法 ======
    def check_auth_status(self):
        """首次运行时检查Google认证状态"""
        token_path = os.path.join(
            os.path.expanduser("~"), 
            ".amazon-processor", 
            "token.pickle"
        )
        
        if not os.path.exists(token_path):
            response = messagebox.askyesno(
                "First-time Authorization",
                "This application requires Google Account authorization to access Google Sheets.\nProceed now?",
                icon='question'
            )
            if response:
                try:
                    # 触发授权流程
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
            

            # ========== 新增代码：加载成本表 ==========
            # 加载两个Google Sheet
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
                                            
                                        # 计算product_cost（注意缩进层级）
                                        grouped['product_cost'] = grouped['master_sku'].apply(
                                            lambda sku: (  # 括号开始
                                                0.0 
                                                if str(sku).strip().lower() == "shipping"  # 条件判断
                                                else landed_cost_data.get(  # 函数调用换行缩进
                                                    str(sku).strip(),  # 参数1（4空格缩进）
                                                    pdb_us_data.get(str(sku).strip(), None)  # 参数2（与参数1对齐）
                                                )  # get方法闭合
                                            )  # lambda表达式闭合
                                        )

                                        # 计算total_cost（与上一代码块同级缩进）
                                        grouped['total_cost'] = grouped['product_cost'] * grouped['total QTY']

                                            # ========== 新增代码：添加Shipping汇总行 ==========
                                        try:
                                               # 获取当月总运费（从order_details数据）
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
            
                                            # 合并新行（确保列顺序一致）
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

                                    # 计算product_cost（注意缩进层级）
                                    grouped['product_cost'] = grouped['master_sku'].apply(
                                        lambda sku: (  # 括号开始
                                            0.0 
                                            if str(sku).strip().lower() == "shipping"  # 条件判断
                                            else landed_cost_data.get(  # 函数调用换行缩进
                                                str(sku).strip(),  # 参数1（4空格缩进）
                                                pdb_us_data.get(str(sku).strip(), None)  # 参数2（与参数1对齐）
                                            )  # get方法闭合
                                        )  # lambda表达式闭合
                                    )

                                    # 计算total_cost（与上一代码块同级缩进）
                                    grouped['total_cost'] = grouped['product_cost'] * grouped['total QTY']
                                    

                                    # ========== 新增代码开始 ========== （与try同级缩进）
                                    try:
                                        # 添加Shipping行逻辑    
                                            # ========== 新增代码：添加Shipping汇总行 ==========
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

                                    # 列顺序调整（与 grouped 生成代码同级缩进）
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
    
    

if __name__ == "__main__":
    app = AmazonProcessor()
    app.mainloop()