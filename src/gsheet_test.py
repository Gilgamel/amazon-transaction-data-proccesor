import os
import sys
import gspread
import requests
from googleapiclient import discovery
from oauth2client.service_account import ServiceAccountCredentials

def get_resource_path(relative_path):
    """智能资源路径定位（已修复括号问题）"""
    if getattr(sys, 'frozen', False):
        base_path = sys._MEIPASS
    else:
        # 计算项目根目录（amazon目录）
        # 从src目录 -> 父目录（amazon/src的父目录是amazon）
        base_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    
    full_path = os.path.join(base_path, relative_path)
    print(f"[路径追踪] 资源解析：{full_path}")
    return full_path

def validate_environment():
    """系统环境验证"""
    print("="*50)
    print("Google Sheet连接专家诊断工具 v3.1")
    print("="*50)
    
    print("\n[系统环境]")
    print(f"Python版本: {sys.version.split()[0]}")
    print(f"工作目录: {os.getcwd()}")
    print(f"执行路径: {sys.executable}")

def test_network_connection():
    """网络连通性深度测试"""
    print("\n[网络诊断]")
    test_points = {
        "Google身份验证": "https://accounts.google.com",
        "Google Sheets API": "https://sheets.googleapis.com/v4/spreadsheets",
        "Google Drive API": "https://www.googleapis.com/drive/v3/files"
    }
    
    for name, url in test_points.items():
        try:
            resp = requests.head(url, timeout=10)
            status = "✓ 连通" if resp.status_code < 400 else "⚠️ 受限"
            print(f"{name.ljust(15)}: {status} (HTTP {resp.status_code})")
        except Exception as e:
            print(f"{name.ljust(15)}: ❌ 失败 ({str(e)})")

def initialize_google_client():
    """Google服务初始化"""
    print("\n[服务初始化]")
    credentials_path = get_resource_path("resources/auth/credentials.json")
    
    # 凭证文件验证
    if not os.path.exists(credentials_path):
        raise FileNotFoundError(f"凭证文件缺失：{credentials_path}")
    print(f"✓ 凭证文件验证通过（大小：{os.path.getsize(credentials_path)}字节）")

    # 权限作用域配置
    SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive',
    'https://www.googleapis.com/auth/drive.metadata.readonly'
]
    
    try:
        creds = ServiceAccountCredentials.from_json_keyfile_name(credentials_path, SCOPES)
        client = gspread.authorize(creds)
        drive_service = discovery.build('drive', 'v3', credentials=creds)
        print(f"✓ 服务账号认证成功：{creds.service_account_email}")
        return client, drive_service
    except Exception as e:
        print(f"❌ 认证失败：{type(e).__name__} - {str(e)}")
        raise

def full_connection_test():
    """全面连接测试"""
    try:
        client, drive_service = initialize_google_client()
        TARGET_NAME = "SKU Manual Mapping"
        TARGET_ID = "1moD4asH6Qg097ffgrNjEeJYr6PYV9Tjj86RlcI3vF7o"  # 可选：在此填写表格ID
        
        print("\n[测试1] 通过名称访问")
        try:
            spreadsheet = client.open(TARGET_NAME)
            print(f"✓ 访问成功！表格ID: {spreadsheet.id}")
            print(f"  首行数据: {spreadsheet.sheet1.row_values(1)}")
            return
        except gspread.SpreadsheetNotFound:
            print("⚠️ 名称访问失败，开始深度排查...")

        print("\n[测试2] 通过Drive API搜索")
        results = drive_service.files().list(
            q=f"name='{TARGET_NAME}' and mimeType='application/vnd.google-apps.spreadsheet'",
            fields="files(id, name, webViewLink)",
            corpora="allDrives",
            includeItemsFromAllDrives=True,
            supportsAllDrives=True
        ).execute()
        
        if files := results.get('files', []):
            print(f"找到 {len(files)} 个匹配文件：")
            for file in files:
                print(f"  - {file['name']} (ID: {file['id']})")
                print(f"    访问链接: {file['webViewLink']}")
        else:
            print("⚠️ 未找到匹配文件")

        print("\n[测试3] 所有可见表格列表")
        try:
            sheets = client.openall()
            print(f"共找到 {len(sheets)} 个表格：")
            for sheet in sheets:
                print(f"  - {sheet.title} (ID: {sheet.id})")
        except Exception as e:
            print(f"列表获取失败: {str(e)}")

    except Exception as e:
        print(f"\n❌ 测试失败：{type(e).__name__} - {str(e)}")
        print("\n排查建议：")
        if "invalid_grant" in str(e):
            print("1. 检查系统时间是否准确（控制面板->日期和时间）")
            print("2. 重新生成服务账号密钥")
        elif "Unable to discover service" in str(e):
            print("1. 运行: pip install --upgrade google-api-python-client")
        else:
            print("1. 确认表格已正确分享给服务账号")
            print("2. 在Google Cloud启用Sheets API和Drive API")

if __name__ == "__main__":
    validate_environment()
    test_network_connection()
    full_connection_test()