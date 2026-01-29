"""
工具函數模組
包含日誌配置、日期處理等共用工具
"""
import os
import yaml
import logging
from datetime import date, timedelta
from pathlib import Path
from typing import Dict, Tuple
from dotenv import load_dotenv


def setup_logging(config: Dict, log_dir: str = "./logs") -> None:
    """
    設定日誌系統
    
    Args:
        config: 日誌配置
        log_dir: 日誌目錄
    """
    os.makedirs(log_dir, exist_ok=True)
    
    # 生成日誌檔名
    log_file = os.path.join(
        log_dir, 
        f"{config.get('file_prefix', 'app')}_{date.today().strftime('%Y%m%d')}.log"
    )
    
    # 取得 root logger
    root_logger = logging.getLogger()
    
    # 清除現有的 handlers（避免重複）
    if root_logger.handlers:
        for handler in root_logger.handlers[:]:
            root_logger.removeHandler(handler)
    
    # 設定日誌等級
    root_logger.setLevel(config.get('level', 'INFO'))
    
    # 設定格式
    formatter = logging.Formatter(
        config.get('format', '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    )
    
    # 檔案 handler
    file_handler = logging.FileHandler(log_file, encoding='utf-8')
    file_handler.setLevel(config.get('level', 'INFO'))
    file_handler.setFormatter(formatter)
    root_logger.addHandler(file_handler)
    
    # 終端機 handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(config.get('level', 'INFO'))
    console_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)
    
    # 抑制第三方套件的日誌
    logging.getLogger('openai').setLevel(logging.WARNING)
    logging.getLogger('httpx').setLevel(logging.WARNING)
    logging.getLogger('urllib3').setLevel(logging.WARNING)
    
    # 測試日誌是否正常
    root_logger.info(f"日誌系統初始化完成，日誌檔案: {log_file}")


def load_config(config_path: str = "./config/config.yaml") -> Dict:
    """
    載入配置檔
    
    Args:
        config_path: 配置檔路徑
        
    Returns:
        配置字典
    """
    with open(config_path, 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)
    return config


def load_env_variables(env_path: str = "./config/.env") -> None:
    """
    載入環境變數
    
    Args:
        env_path: .env 檔案路徑
    """
    if os.path.exists(env_path):
        load_dotenv(env_path)
        logging.info(f"✅ 已載入環境變數: {env_path}")
    else:
        logging.warning(f"⚠️  .env 檔案不存在: {env_path}")
        logging.warning("提示：請複製 config/.env.example 為 config/.env 並填入帳密")


def get_date_range(days_back: int = None) -> Tuple[str, str]:
    """
    獲取日期範圍（用於資料查詢）
    
    Args:
        days_back: 向前推幾天（None 則自動判斷週末）
        
    Returns:
        (開始日期, 結束日期) 格式: YYYY/MM/DD
    """
    today = date.today()
    
    if days_back is not None:
        date_bgn = today - timedelta(days=days_back)
        date_end = today - timedelta(days=1)
    else:
        # 週一則往前推 3 天，其他推 2 天
        if today.weekday() == 0:  # 週一
            date_bgn = today - timedelta(days=3)
            date_end = today - timedelta(days=1)
        else:
            date_bgn = today - timedelta(days=2)
            date_end = today - timedelta(days=1)
    
    return (
        date_bgn.strftime("%Y/%m/%d"),
        date_end.strftime("%Y/%m/%d")
    )


def save_log_parameter(date_bgn: str, date_end: str, 
                      log_file: str = "./logs/log_parameter.yaml") -> None:
    """
    儲存執行參數到日誌檔
    
    Args:
        date_bgn: 開始日期
        date_end: 結束日期
        log_file: 日誌檔案路徑
    """
    os.makedirs(os.path.dirname(log_file), exist_ok=True)
    
    log_parameter = {
        'daliy_batch_news_date_bgn': date_bgn,
        'daliy_batch_news_date_end': date_end
    }
    
    with open(log_file, 'w', encoding='utf-8') as f:
        yaml.safe_dump(log_parameter, f, allow_unicode=True)


def ensure_directories(*dirs) -> None:
    """
    確保目錄存在，不存在則創建
    
    Args:
        *dirs: 目錄路徑列表
    """
    for dir_path in dirs:
        os.makedirs(dir_path, exist_ok=True)
