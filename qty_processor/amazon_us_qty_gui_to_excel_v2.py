import tkinter as tk
from tkinter import filedialog, messagebox
from tkcalendar import Calendar
import pandas as pd
from datetime import datetime

# 定义处理数据的函数
def process_file(file_path, start_date, end_date):
    try:
        # 读取文件
        df = pd.read_csv(file_path, delimiter='\t', encoding='utf-8')

        # 删除首行
        df = df.drop(index=0)
        
        # 打印列名和前几行数据进行检查
        print("Columns in the file:", df.columns)
        print("First few rows of the data:\n", df.head())

        # 检查是否存在 'posted-date' 列
        if 'posted-date' not in df.columns:
            raise ValueError("The 'posted-date' column is missing from the file.")

        # 转换 'posted-date' 为日期格式，处理无效日期
        df['posted-date'] = pd.to_datetime(df['posted-date'], errors='coerce')  # 转换为日期格式
        print("After conversion, first few rows of posted-date:\n", df['posted-date'].head())

        # 删除无效日期（NaT）
        df = df.dropna(subset=['posted-date'])

        # 获取文件中的日期范围
        min_date = df['posted-date'].min()
        max_date = df['posted-date'].max()

        # 根据日期范围筛选数据
        df = df[(df['posted-date'] >= start_date) & (df['posted-date'] <= end_date)]

        # 更改列类型
        df = df.astype({
            "settlement-id": 'Int64',
            "settlement-start-date": 'str',
            "settlement-end-date": 'str',
            "deposit-date": 'str',
            "total-amount": 'float',
            "currency": 'str',
            "transaction-type": 'str',
            "order-id": 'str',
            "merchant-order-id": 'str',
            "adjustment-id": 'str',
            "shipment-id": 'str',
            "marketplace-name": 'str',
            "amount-type": 'str',
            "amount-description": 'str',
            "amount": 'float',
            "fulfillment-id": 'str',
            "posted-date": 'str',
            "posted-date-time": 'str',
            "order-item-code": 'Int64',
            "merchant-order-item-id": 'str',
            "merchant-adjustment-item-id": 'str',
            "sku": 'str',
            "quantity-purchased": 'Int64',
            "promotion-id": 'str'
        })

        # 默认筛选 marketplace 为 'Amazon.com'
        df = df[(df['transaction-type'] == 'Order') & (df['marketplace-name'] == 'Amazon.com')]

        # 删除不需要的列
        df = df.drop(columns=["settlement-id", "settlement-start-date", "settlement-end-date", "deposit-date", "total-amount", 
                               "currency", "transaction-type", "merchant-order-id", "adjustment-id", "marketplace-name", 
                               "fulfillment-id", "posted-date", "posted-date-time", "order-item-code", "merchant-order-id", 
                               "merchant-adjustment-item-id", "promotion-id"])

        # 重新排列列
        df = df[["order-id", "shipment-id", "amount-description", "amount-type", "amount", "sku", "quantity-purchased"]]

        # 合并 "amount-description" 和 "amount-type" 为 "des-type"
        df['des-type'] = df['amount-description'] + ":" + df['amount-type']

        # 筛选 "des-type" 为 "Principal:ItemPrice"
        df = df[df['des-type'] == "Principal:ItemPrice"]

        # 删除 "amount" 列
        df = df.drop(columns=["amount"])

        # 分组并求和
        df = df.groupby(["order-id", "shipment-id", "des-type", "sku"], as_index=False)["quantity-purchased"].sum()

        # 删除 "des-type" 列
        df = df.drop(columns=["des-type"])

        # 排序
        df = df.sort_values(by=["shipment-id"], ascending=True)

        return df, min_date, max_date
    except Exception as e:
        print(f"Error processing file: {e}")  # 输出错误信息
        messagebox.showerror("Error", f"Error processing file: {e}")
        return None, None, None

# 打开文件选择对话框
def open_file_dialog():
    file_path = filedialog.askopenfilename(filetypes=[("Text files", "*.txt")])
    if file_path:
        file_path_var.set(file_path)

        # 自动读取日期并更新日期选择器
        df, min_date, max_date = process_file(file_path, datetime.min, datetime.max)
        if df is not None:
            # 自动更新日期选择器为文件中的最小和最大日期
            start_date_picker.selection_set(min_date)  # 更新开始日期
            end_date_picker.selection_set(max_date)    # 更新结束日期

            # 解锁日期选择器，允许用户修改日期
            start_date_picker.config(state="normal")
            end_date_picker.config(state="normal")

# 打开保存文件对话框
def save_file_dialog():
    save_path = filedialog.asksaveasfilename(defaultextension=".xlsx", filetypes=[("Excel files", "*.xlsx")])
    if save_path:
        save_path_var.set(save_path)

# 提交按钮回调函数
def on_submit():
    file_path = file_path_var.get()
    save_path = save_path_var.get()
    if not file_path:
        messagebox.showwarning("Warning", "Please select a file.")
        return

    if not save_path:
        messagebox.showwarning("Warning", "Please select a save location.")
        return

    # 获取选择的日期
    start_date = start_date_picker.get_date()
    end_date = end_date_picker.get_date()

    # 处理文件
    df, _, _ = process_file(file_path, start_date, end_date)
    if df is not None:
        try:
            # 保存为 Excel 文件
            df.to_excel(save_path, index=False)
            messagebox.showinfo("Success", f"File saved successfully to {save_path}")
        except Exception as e:
            messagebox.showerror("Error", f"Error saving file: {e}")
    else:
        messagebox.showerror("Error", "No data to save.")

# 创建 GUI
root = tk.Tk()
root.title("US Amazon File Processor")

# 文件路径选择
file_path_var = tk.StringVar()
file_path_label = tk.Label(root, text="Select file:")
file_path_label.pack(pady=5)

file_path_entry = tk.Entry(root, textvariable=file_path_var, width=50)
file_path_entry.pack(pady=5)

browse_button = tk.Button(root, text="Browse", command=open_file_dialog)
browse_button.pack(pady=5)

# 保存文件路径选择
save_path_var = tk.StringVar()
save_path_label = tk.Label(root, text="Select save location:")
save_path_label.pack(pady=5)

save_path_entry = tk.Entry(root, textvariable=save_path_var, width=50)
save_path_entry.pack(pady=5)

save_button = tk.Button(root, text="Save As", command=save_file_dialog)
save_button.pack(pady=5)

# 日期选择
date_label = tk.Label(root, text="Select date range:")
date_label.pack(pady=5)

start_date_label = tk.Label(root, text="Start Date:")
start_date_label.pack(pady=5)

start_date_picker = Calendar(root, date_pattern="yyyy-mm-dd")
start_date_picker.pack(pady=5)

end_date_label = tk.Label(root, text="End Date:")
end_date_label.pack(pady=5)

end_date_picker = Calendar(root, date_pattern="yyyy-mm-dd")
end_date_picker.pack(pady=5)

# 提交按钮
submit_button = tk.Button(root, text="Submit", command=on_submit)
submit_button.pack(pady=20)

# 启动 GUI
root.mainloop()
