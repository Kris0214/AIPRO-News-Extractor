"""
Utility Functions Module
Includes logging configuration, date handling and other shared utilities
"""
import os
import yaml
import logging
from datetime import date, timedelta
from pathlib import Path
from typing import Dict, Tuple, Optional, Any
from dotenv import load_dotenv


def setup_logging(config: Dict[str, Any], log_dir: str = "./logs") -> None:
    """
    Setup logging system.
    
    Args:
        config: Logging configuration
        log_dir: Log directory
    """
    os.makedirs(log_dir, exist_ok=True)
    
    # Generate log filename
    log_file = os.path.join(
        log_dir, 
        f"{config.get('file_prefix', 'aipro_news')}_{date.today().strftime('%Y%m%d')}.log"
    )
    
    # Get root logger
    root_logger = logging.getLogger()
    
    # Clear existing handlers (avoid duplication)
    if root_logger.handlers:
        for handler in root_logger.handlers[:]:
            root_logger.removeHandler(handler)
    
    # Set log level
    root_logger.setLevel(config.get('level', 'INFO'))
    
    # Set format
    formatter = logging.Formatter(
        config.get('format', '%(asctime)s - %(levelname)s - %(message)s')
    )
    
    # File handler
    file_handler = logging.FileHandler(log_file, encoding='utf-8')
    file_handler.setLevel(config.get('level', 'INFO'))
    file_handler.setFormatter(formatter)
    root_logger.addHandler(file_handler)
    
    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(config.get('level', 'INFO'))
    console_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)
    
    # Suppress third-party package logs
    logging.getLogger('openai').setLevel(logging.WARNING)
    logging.getLogger('httpx').setLevel(logging.WARNING)
    logging.getLogger('urllib3').setLevel(logging.WARNING)
    
    # Test if logging works properly
    root_logger.info(f"Logging system initialized, log file: {log_file}")


def load_config(config_path: str = "./config/config.yaml") -> Dict[str, Any]:
    """
    Load configuration file.
    
    Args:
        config_path: Configuration file path
        
    Returns:
        Configuration dictionary
    """
    with open(config_path, 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)
    return config


def load_env_variables(env_path: str = "./config/.env") -> None:
    """
    Load environment variables.
    
    Args:
        env_path: .env file path
    """
    if os.path.exists(env_path):
        load_dotenv(env_path)
        logging.info(f"Environment variables loaded: {env_path}")
    else:
        logging.warning(f".env file does not exist: {env_path}")
        logging.warning("Hint: Copy config/.env.example to config/.env and fill in credentials")


def get_date_range(days_back: Optional[int] = None) -> Tuple[str, str]:
    """
    Get date range (for data query).
    
    Args:
        days_back: Days to go back (None for auto weekend detection)
        
    Returns:
        (start_date, end_date) format: YYYY/MM/DD
    """
    today = date.today()
    
    if days_back is not None:
        date_bgn = today - timedelta(days=days_back)
        date_end = today - timedelta(days=1)
    else:
        # If Monday, go back 3 days; otherwise 2 days
        if today.weekday() == 0:  # Monday
            date_bgn = today - timedelta(days=3)
            date_end = today - timedelta(days=1)
        else:
            date_bgn = today - timedelta(days=2)
            date_end = today - timedelta(days=1)
    
    return (
        date_bgn.strftime("%Y/%m/%d"),
        date_end.strftime("%Y/%m/%d")
    )


def load_prompt(filename: str, prompts_dir: str = "./prompts") -> str:
    """
    Load prompt from text file.
    
    Args:
        filename: Prompt filename (with or without .txt extension)
        prompts_dir: Directory containing prompt files
        
    Returns:
        Prompt content as string
    """
    if not filename.endswith('.txt'):
        filename = f"{filename}.txt"
    
    filepath = os.path.join(prompts_dir, filename)
    
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            return f.read()
    except FileNotFoundError:
        logging.error(f"Prompt file not found: {filepath}")
        raise
    except Exception as e:
        logging.error(f"Failed to load prompt {filename}: {e}")
        raise
