"""
資料庫操作模組
負責與 Oracle ODS 資料庫的連線和資料擷取
"""
import time
import logging
import pandas as pd
import oracledb
from contextlib import contextmanager
from typing import Optional

logger = logging.getLogger(__name__)


class DatabaseManager:
    """Oracle 資料庫管理器"""
    
    def __init__(self, account: str, password: str, host: str, port: str, 
                 service_name: str, oracle_client_path: str):
        """
        初始化資料庫管理器
        
        Args:
            account: 資料庫帳號
            password: 資料庫密碼
            host: 主機位址
            port: 連接埠
            service_name: 服務名稱
            oracle_client_path: Oracle Client 路徑
        """
        self.account = account
        self.password = password
        self.host = host
        self.port = port
        self.service_name = service_name
        self.oracle_client_path = oracle_client_path
        
        # 初始化 Oracle Client
        try:
            oracledb.init_oracle_client(lib_dir=oracle_client_path)
            logger.info(f"✅ Oracle Client 初始化成功: {oracle_client_path}")
        except Exception as e:
            logger.warning(f"⚠️  Oracle Client 初始化警告: {e}")
            logger.warning(f"檢查路徑: {oracle_client_path}")
    
    @contextmanager
    def get_connection(self):
        """
        獲取資料庫連線的 context manager
        
        Yields:
            資料庫連線物件
        """
        dsn = f"{self.host}:{self.port}/{self.service_name}"
        conn = None
        try:
            conn = oracledb.connect(
                user=self.account,
                password=self.password,
                dsn=dsn
            )
            logger.debug("資料庫連線建立成功")
            yield conn
        except Exception as e:
            logger.error(f"❌ 資料庫連線失敗: {e}")
            logger.error(f"DSN: {dsn}")
            logger.error(f"帳號: {self.account}")
            logger.error(f"Oracle Client: {self.oracle_client_path}")
            raise
        finally:
            if conn:
                conn.close()
                logger.debug("資料庫連線已關閉")
    
    def test_connection(self) -> bool:
        """
        測試資料庫連線是否正常
        
        Returns:
            連線成功返回 True，否則 False
        """
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT SYSDATE FROM DUAL")
                cursor.fetchone()
                logger.info("資料庫連線測試成功")
                return True
        except Exception as e:
            logger.error(f"資料庫連線測試失敗: {e}")
            return False
    
    def fetch_dataframe(self, query: str, process_clob: bool = True) -> pd.DataFrame:
        """
        執行 SQL 查詢並返回 DataFrame（通用方法）
        
        Args:
            query: SQL 查詢語句
            process_clob: 是否自動讀取 CLOB 欄位內容（預設 True）
            
        Returns:
            查詢結果的 DataFrame
        """
        start_time = time.time()
        
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(query)
                
                # 取得欄位名稱
                columns = [desc[0] for desc in cursor.description]
                
                # 取得資料
                rows = cursor.fetchall()
                
                if not rows:
                    logger.warning("查詢結果為空")
                    return pd.DataFrame(columns=columns)
                
                # 處理資料（支援 CLOB）
                if process_clob:
                    processed_rows = []
                    for row in rows:
                        processed_row = []
                        for cell in row:
                            if hasattr(cell, 'read'):
                                processed_row.append(cell.read())
                            else:
                                processed_row.append(cell)
                        processed_rows.append(processed_row)
                    data = processed_rows
                else:
                    data = rows
                
                # 建立 DataFrame
                df = pd.DataFrame(data, columns=columns)
                
                elapsed_time = time.time() - start_time
                logger.info(f"查詢完成，共 {len(df)} 筆資料，耗時 {elapsed_time:.2f} 秒")
                
                return df
                
        except Exception as e:
            logger.error(f"查詢執行失敗: {e}")
            raise