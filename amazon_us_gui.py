import pandas as pd
import tkinter as tk
from tkinter import filedialog, messagebox

# 定义处理逻辑 1
def process_first_logic(df):
    df_filtered = df[(df['transaction_type'] == 'Order') & 
                     (df['amount_type'].isin(['ItemPrice', 'ItemWithheldTax', 'Promotion'])) & 
                     (df['marketplace_name'] == 'Amazon.com')]

    df_filtered['des_type'] = df_filtered['amount_description'] + ':' + df_filtered['amount_type']
    df_filtered = df_filtered.drop(columns=['amount_description', 'amount_type'])
    df_filtered = df_filtered.sort_values(by='shipment_id')

    df_filtered['Total_amount'] = df_filtered['amount'].fillna(0)
    df_filtered = df_filtered.fillna(0)
    
    df_filtered['Product_Amount'] = df_filtered['amount']
    df_filtered['Product_Tax'] = df_filtered['Product_Amount'] * 0.1
    df_filtered['Total_Amount'] = df_filtered['Product_Amount'] + df_filtered['Product_Tax']
    df_filtered['Shipping'] = df_filtered['amount']
    df_filtered['Shipping_Tax'] = df_filtered['Shipping'] * 0.05
    df_filtered['Total_Shipping'] = df_filtered['Shipping'] + df_filtered['Shipping_Tax']
    df_filtered['Tax_Rate'] = df_filtered['Product_Tax'] / df_filtered['Product_Amount']
    
    return df_filtered

# 定义处理逻辑 2
def process_second_logic(df):
    df_filtered = df[(df['transaction_type'] == 'Order') & 
                     (df['amount_type'].isin(['ItemPrice', 'ItemWithheldTax', 'Promotion'])) & 
                     (df['marketplace_name'] == 'Amazon.com')]

    df_filtered['des_type'] = df_filtered['amount_description'] + ':' + df_filtered['amount_type']
    df_filtered = df_filtered.drop(columns=['amount_description', 'amount_type'])
    df_filtered = df_filtered.sort_values(by='shipment_id')

    df_filtered['Shipping Tax'] = (
        (df_filtered['ShippingTax:ItemPrice'].fillna(0)) +
        (df_filtered['MarketplaceFacilitatorTax-Shipping:ItemWithheldTax'].fillna(0)) +
        (df_filtered['MarketplaceFacilitatorVATTax-Shipping:ItemWithheldTax'].fillna(0)) +
        (df_filtered['LowValueGoodsTax-Shipping:ItemWithheldTax'].fillna(0)) +
        (df_filtered['MarketplaceFacilitatorVAT-Shipping:ItemWithheldTax'].fillna(0))
    )

    df_filtered['Product_Amount'] = df_filtered['Principal:ItemPrice'].fillna(0) + df_filtered['Principal:Promotion'].fillna(0)
    df_filtered = df_filtered.drop(columns=["Principal:ItemPrice", "Principal:Promotion"])

    df_filtered['Product_Tax'] = (
        df_filtered['Tax:ItemPrice'].fillna(0) +
        df_filtered['MarketplaceFacilitatorTax-Principal:ItemWithheldTax'].fillna(0) +
        df_filtered['MarketplaceFacilitatorVAT-Principal:ItemWithheldTax'].fillna(0) +
        df_filtered['LowValueGoodsTax-Principal:ItemWithheldTax'].fillna(0)
    )

    df_filtered['Shipping'] = df_filtered['Shipping:ItemPrice'].fillna(0) + df_filtered['Shipping:Promotion'].fillna(0)
    df_filtered['Giftwrap'] = df_filtered['GiftWrap:ItemPrice'].fillna(0) + df_filtered['GiftWrap:Promotion'].fillna(0)
    df_filtered['Giftwrap Tax'] = df_filtered['GiftWrapTax:ItemPrice'].fillna(0) + df_filtered['MarketplaceFacilitatorTax-Other:ItemWithheldTax'].fillna(0)

    df_filtered['Total_amount'] = (
        df_filtered['Product_Amount'] + df_filtered['Product_Tax'] +
        df_filtered['Giftwrap'] + df_filtered['Giftwrap Tax']
    )

    df_filtered['Total_shipping'] = df_filtered['Shipping'] + df_filtered['Shipping Tax']
    df_filtered['tax_rate'] = df_filtered['Product_Tax'] / df_filtered['Product_Amount']

    return df_filtered

# 定义按钮点击事件
def select_file():
    global file_path
    file_path = filedialog.askopenfilename(title="Select a CSV File", filetypes=[("Text files", "*.txt")])
    if file_path:
        file_label.config(text=f"File Selected: {file_path}")

def save_file(df, title):
    output_file = filedialog.asksaveasfilename(defaultextension=".xlsx", filetypes=[("Excel Files", "*.xlsx")], title=title)
    if output_file:
        df.to_excel(output_file, index=False)
        messagebox.showinfo("Success", f"File saved successfully to {output_file}")

def process_file():
    if not file_path:
        messagebox.showerror("Error", "Please select a file first!")
        return

    # 读取文件
    try:
        df = pd.read_csv(file_path, delimiter='\t', encoding='utf-8')
    except Exception as e:
        messagebox.showerror("Error", f"Failed to read file: {e}")
        return

    # 处理第一种逻辑
    df_first_logic = process_first_logic(df)
    save_file(df_first_logic, "Save First Logic Excel File")

    # 处理第二种逻辑
    df_second_logic = process_second_logic(df)
    save_file(df_second_logic, "Save Second Logic Excel File")

# 创建GUI窗口
root = tk.Tk()
root.title("Data Processing Tool")

# 设置窗口大小
root.geometry("400x300")

# 文件选择按钮
select_button = tk.Button(root, text="Select a .txt file", command=select_file)
select_button.pack(pady=20)

# 显示选择的文件
file_label = tk.Label(root, text="No file selected")
file_label.pack(pady=10)

# 处理文件按钮
process_button = tk.Button(root, text="Process File", command=process_file)
process_button.pack(pady=20)

# 启动GUI
root.mainloop()
