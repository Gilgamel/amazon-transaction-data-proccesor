
**For internal company use only**

The py file of this project has been packaged into exe, which can be used by anyone without any programming backgound.

</br>

**Functions:**
1. Verify if the total amount is correct.
2. Generate a pivot table by month for transaction-type, amount-type, and amount for subsequent expense calculations.
3. Generate order details by month.
4. Match with the SKU manual mapping Excel file on Google via API.
5. Summarize quantity and total amount based on SKU.

</br>

**Code used to package exe file.**

```
pyinstaller --noconfirm --onefile --windowed `
   --add-data "resources/icon;resources/icon" `
   --icon "resources/icon/app.ico" `
   "src/amazon_us_qty_order.py"
```

```
pyinstaller --noconfirm --onefile --windowed `
  --add-data "resources/icon;resources/icon" `
  --add-data ".env;." `
  --hidden-import "google.auth.transport.requests" `
  --hidden-import "google_auth_oauthlib.flow" `
  --icon "resources/icon/app.ico" `
  --name "AmazonProcessor" `
  "src/amazon_us_qty_order.py"
```