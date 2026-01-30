import os, sys, logging
from pathlib import Path
from typing import Optional
from datetime import datetime

sys.path.insert(0, str(Path(__file__).parent / "src"))

from src.database import DatabaseManager
from src.llm_service import LLMService
from src.news_service import NewsService
from utils.utils import setup_logging, load_config, load_env_variables, get_date_range

logger = logging.getLogger(__name__)

def main():
    """
    Main entry point for AIPRO News Processing System.
    """
    try:
        # 1. setup configuration
        config = load_config("./config/config.yaml")
        load_env_variables("./config/.env")
        
        # 2. setup logging
        setup_logging(
            config['logging'],
            config['paths']['logs_dir']
        )
        
        logger.info("AIPRO News Processing System Started")
        start_time = datetime.now()

        # 3. Ensure required directories exist
        os.makedirs(config['paths']['outputs_dir'], exist_ok=True)
        os.makedirs(config['paths']['logs_dir'], exist_ok=True)
        
        # 4. Get date range
        date_bgn, date_end = get_date_range()
        logger.info(f"Processing date range: {date_bgn} ~ {date_end}")
        
        # 5. Initialize database manager
        logger.info("Initializing database connection...")
        db_manager = DatabaseManager(
            account=os.getenv('ODS_ACCOUNT') or '',
            password=os.getenv('ODS_PASSWORD') or '',
            host=config['database']['host'],
            port=config['database']['port'],
            service_name=config['database']['service_name'],
            oracle_client_path=config['database']['oracle_client_path']
        )
        
        # 6. Initialize LLM service
        logger.info("Initializing LLM service...")
        llm_service = LLMService(
            endpoint=config['azure_openai']['endpoint'],
            api_key=os.getenv('AOAI_API_KEY') or '',
            api_version=config['azure_openai']['api_version'],
            model=config['azure_openai']['model'],
            max_tokens=config['azure_openai']['max_tokens'],
            temperature=config['azure_openai']['temperature'],
            timeout=config['news']['timeout']
        )
        
        # 7. Initialize news service
        news_service = NewsService(
            db_manager=db_manager,
            llm_service=llm_service,
            config=config['news']
        )
        
        # 8. Execute news processing
        logger.info("Starting news processing...")
        df_news = news_service.process_daily_news(
            date_bgn=date_bgn,
            date_end=date_end,
        )
        
        # 9. Save results
        if len(df_news) > 0:
            news_service.save_news_data(
                df_news=df_news,
                date_end=date_end,
                output_dir=config['paths']['outputs_dir']
            )    
        else:
            logger.warning("No valid news data")

        endtime = datetime.now()
        duration = endtime - start_time
        logger.info(f"Execution completed! Executed duration: {round(duration.total_seconds(), 2)} sec")
        
    except KeyboardInterrupt:
        logger.warning("Program interrupted by user")
        
    except Exception as e:
        logger.error(f"Program execution failed: {e}", exc_info=True)


if __name__ == "__main__":
    main()
