import os
import sys
import pickle
import webbrowser
from tkinter import messagebox
from dotenv import load_dotenv
from google_auth_oauthlib.flow import InstalledAppFlow
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request

def load_environment():
    """安全加载环境配置"""
    try:
        # 优先从项目根目录加载.env文件
        env_path = os.path.join(os.path.dirname(__file__), '..', '..', '.env')
        if os.path.exists(env_path):
            load_dotenv(dotenv_path=env_path)
        else:
            # 打包后从可执行文件同级目录加载
            if getattr(sys, 'frozen', False):
                base_path = sys._MEIPASS
                env_path = os.path.join(base_path, '.env')
                if os.path.exists(env_path):
                    load_dotenv(dotenv_path=env_path)
    except Exception as e:
        print(f"[环境加载警告] {str(e)}")

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