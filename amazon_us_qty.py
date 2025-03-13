import pandas as pd


# 读取数据（这里是 .txt 文件，使用制表符作为分隔符）
file_path = r"C:\Users\vuser\My Drive\Documents\AR\Amazon Data\US Store (store-V-Edifier)\2025-0211-0225 Standard\2025 0211-0225 Standard $504501.68.txt"

df = pd.read_csv(file_path, delimiter='\t', encoding='utf-8')
df

# Convert column types (using appropriate data types for each column) 
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

# Remove the first row (equivalent to Table.Skip)
df = df.drop(index=0)
df

# Filter the rows (only "Order" transaction-type and "Amazon.com" marketplace-name)
df = df[(df['transaction-type'] == 'Order') & (df['marketplace-name'] == 'Amazon.com')]
df

# Remove unwanted columns
df = df.drop(columns=["settlement-id", "settlement-start-date", "settlement-end-date", "deposit-date", "total-amount", 
                       "currency", "transaction-type", "merchant-order-id", "adjustment-id", "marketplace-name", 
                       "fulfillment-id", "posted-date", "posted-date-time", "order-item-code", "merchant-order-item-id", 
                       "merchant-adjustment-item-id", "promotion-id"])

# Reorder the columns
df = df[["order-id", "shipment-id", "amount-description", "amount-type", "amount", "sku", "quantity-purchased"]]
df

# Merge "amount-description" and "amount-type" columns into "des-type" column
df['des-type'] = df['amount-description'] + ":" + df['amount-type']

# Filter rows where "des-type" is "Principal:ItemPrice"
df = df[df['des-type'] == "Principal:ItemPrice"]

# Remove the "amount" column
df = df.drop(columns=["amount"])

# Group by "order-id", "shipment-id", "des-type", "sku", and sum "quantity-purchased"
df = df.groupby(["order-id", "shipment-id", "des-type", "sku"], as_index=False)["quantity-purchased"].sum()

# Remove the "des-type" column
df = df.drop(columns=["des-type"])

# Sort by "shipment-id"
df = df.sort_values(by=["shipment-id"], ascending=True)

# Final result
print(df)