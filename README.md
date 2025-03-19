


# 
```
pyinstaller -F -w --icon=icon\app.ico --upx-dir=C:\upx .\amazon_us_qty_order.py
```

V2
```
pyinstaller --noconfirm --onefile --windowed `
   --add-data "resources/icon;resources/icon" `
   --icon "resources/icon/app.ico" `
   "src/amazon_us_qty_order.py"
```

