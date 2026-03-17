# aipro_sftp_delivery.py
from __future__ import annotations
 
import os
import posixpath
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional, Sequence
 
import pandas as pd
import paramiko
 
 
# ========== SFTP Client ==========
 
@dataclass
class SFTPConfig:
    # 正式環境：10.81.24.61
    hostname: str = '10.81.70.11'
    port: int = 22
    username: str = 'datascientist.sec'
    password: Optional[str] = 'Fubon16905'
    timeout: int = 20
 
 
class SFTPClient:
    def __init__(self, cfg: SFTPConfig):
        self.cfg = cfg
        self._ssh: Optional[paramiko.SSHClient] = None
        self._sftp: Optional[paramiko.SFTPClient] = None
 
    def __enter__(self) -> "SFTPClient":
        self.connect()
        return self
 
    def __exit__(self, exc_type, exc, tb):
        self.close()
 
    @property
    def sftp(self) -> paramiko.SFTPClient:
        if not self._sftp:
            raise RuntimeError("SFTP not connected")
        return self._sftp
 
    def connect(self) -> None:
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(
            hostname=self.cfg.hostname,
            port=self.cfg.port,
            username=self.cfg.username,
            password=self.cfg.password,
            timeout=self.cfg.timeout,
        )
        self._ssh = ssh
        self._sftp = ssh.open_sftp()
 
    def close(self) -> None:
        if self._sftp:
            self._sftp.close()
            self._sftp = None
        if self._ssh:
            self._ssh.close()
            self._ssh = None
 
    def ensure_remote_dir(self, remote_dir: str) -> None:
        """遞迴建立遠端目錄（POSIX 路徑）"""
        remote_dir = remote_dir.rstrip("/")
        if not remote_dir:
            return
        parts = remote_dir.split("/")
        cur = ""
        for p in parts:
            if not p:
                continue
            cur = cur + "/" + p
            try:
                self.sftp.stat(cur)
            except FileNotFoundError:
                self.sftp.mkdir(cur)
 
    def upload_file(self, local_path: str, remote_path: str, ensure_dir: bool = True) -> None:
        if ensure_dir:
            self.ensure_remote_dir(posixpath.dirname(remote_path))
        self.sftp.put(local_path, remote_path)
 
        # 簡單校驗：檔案大小一致
        local_size = os.path.getsize(local_path)
        remote_size = self.sftp.stat(remote_path).st_size
        if local_size != remote_size:
            raise IOError(f"Upload verification failed: local={local_size}, remote={remote_size}")
 
 
# ========== .D / .OK 生成邏輯 ==========
 
def xlsx_to_d_file(
    xlsx_path: str,
    d_output_path: str,
    sheet_name: Optional[str] = None,
    columns: Optional[Sequence[str]] = None,
    encoding: str = "utf-8",
) -> int:
    """
    把 xlsx 轉成 .D（此處採 CSV 文字檔），回傳資料筆數（列數）。
    - sheet_name: 指定工作表；不指定則讀第一個 sheet
    - columns: 指定欄位順序/子集合；不指定則全部輸出
    """
    df = pd.read_excel(xlsx_path, sheet_name=sheet_name)
 
    if columns is not None:
        df = df[list(columns)]
 
    # 轉 CSV（純文字），副檔名用 .D
    Path(d_output_path).parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(d_output_path, index=False, encoding=encoding)
 
    return int(len(df))
 
 
def build_ok_content(file_code: str, record_count: int, yyyymmdd: str) -> str:
    """
    依你們的規格組 OK 檔內容：
    - 檔案名稱長度 30（靠左，不足補空白）
    - 筆數長度 15（不足補 0）
    - 日期長度 8（yyyyMMdd）
    規格來源：AI PRO 交付 mail 內容。
    """
    return f"{file_code}".ljust(30) + f"{record_count:015d}" + f"{yyyymmdd}"
 
 
def write_ok_file(ok_path: str, file_code: str, record_count: int, yyyymmdd: str, encoding: str = "utf-8") -> None:
    content = build_ok_content(file_code, record_count, yyyymmdd)
    Path(ok_path).parent.mkdir(parents=True, exist_ok=True)
    with open(ok_path, "w", encoding=encoding) as f:
        f.write(content)
 
 
# ========== 對外 API：一行完成 ==========
 
def deliver_xlsx_as_d_and_ok(
    *,
    sftp_cfg: SFTPConfig,
    xlsx_path: str,
    remote_dir: str,
    file_code: str,  # 例如：AIPRO_NEWS / AIPRO_CUSTOMER / AIPRO_PRODRAN
    sheet_name: Optional[str] = None,
    columns: Optional[Sequence[str]] = None,
    local_work_dir: str = "./out_delivery",
    timestamp: Optional[str] = None,
) -> dict:
    """
    1) xlsx -> .D（CSV）
    2) 上傳 .D
    3) 產生 .OK（依規格：檔名30/筆數15/日期8）
    4) 上傳 .OK
 
    回傳：本次產出檔名、筆數、遠端路徑等資訊
    """
    now = datetime.now()
    yyyymmdd = now.strftime("%Y%m%d")
 
    # 你們常見檔名：yyyyMMddHHmmssfff（毫秒 3 位）
    if timestamp is None:
        timestamp = now.strftime("%Y%m%d%H%M%S") + f"{int(now.microsecond/1000):03d}"
 
    d_filename = f"{file_code}_{timestamp}.D"
    ok_filename = f"{file_code}_{timestamp}.OK"
 
    local_work = Path(local_work_dir)
    local_d = str(local_work / d_filename)
    local_ok = str(local_work / ok_filename)
 
    # 1) 轉檔
    record_count = xlsx_to_d_file(
        xlsx_path=xlsx_path,
        d_output_path=local_d,
        sheet_name=sheet_name,
        columns=columns,
        encoding="utf-8",
    )
 
    # 2) 上傳 .D
    remote_dir = remote_dir.rstrip("/")
    remote_d = posixpath.join(remote_dir, d_filename)
    remote_ok = posixpath.join(remote_dir, ok_filename)
 
    with SFTPClient(sftp_cfg) as client:
        client.upload_file(local_d, remote_d, ensure_dir=True)
 
        # 3) 生成 .OK
        write_ok_file(local_ok, file_code=file_code, record_count=record_count, yyyymmdd=yyyymmdd)
 
        # 4) 上傳 .OK（務必在 .D 後）
        client.upload_file(local_ok, remote_ok, ensure_dir=True)
 
    return {
        "file_code": file_code,
        "record_count": record_count,
        "yyyymmdd": yyyymmdd,
        "local_d": local_d,
        "local_ok": local_ok,
        "remote_d": remote_d,
        "remote_ok": remote_ok,
    }
 