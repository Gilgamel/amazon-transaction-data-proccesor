import pandas as pd
import numpy as np
from datetime import datetime
from tkinter import messagebox

from .google_sheets import add_master_sku_from_gsheet

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

def generate_summary(raw_df, start_date, end_date):
    """生成交易类型汇总表"""
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
    """订单表处理"""
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