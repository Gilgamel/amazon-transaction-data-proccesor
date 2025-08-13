# -*- mode: python ; coding: utf-8 -*-


a = Analysis(
    ['src\\amazon_ca_qty_order.py'],
    pathex=[],
    binaries=[],
    datas=[('resources/icon', 'resources/icon'), ('.env', '.')],
    hiddenimports=['google.auth.transport.requests', 'google_auth_oauthlib.flow'],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[],
    noarchive=False,
    optimize=0,
)
pyz = PYZ(a.pure)

exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.datas,
    [],
    name='AmazonProcessor',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    upx_exclude=[],
    runtime_tmpdir=None,
    console=False,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
    icon=['resources\\icon\\app.ico'],
)
