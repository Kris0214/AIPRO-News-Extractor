import os, sys, logging
from pathlib import Path
from typing import Optional
from datetime import datetime

sys.path.insert(0, str(Path(__file__).parent / "src"))
from src.Advisory_reports.adreports_process import AdReports_process
from src.Advisory_reports.adreports_llm import AdReportsLLMService
from utils.database import DatabaseManager
from utils.llm_service import LLMService
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
        
        logger.info("AIPRO Advisory Reports Processing System Started")
        start_time = datetime.now()

        # 3. Ensure required directories exist
        os.makedirs(config['paths']['reports_outputs_dir'], exist_ok=True)
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
            timeout=config['news']['timeout'],
        )
  
        # 7. Initialize advisory reports LLM service (specific)  # ← 新增
        logger.info("Initializing Advisory Reports LLM service...")
        advisory_reports_llm_service = AdReportsLLMService(llm_service)

        # 7. Initialize advisory reports service
        advisory_reports_service = AdReports_process(
            db_manager=db_manager,
            advisory_reports_llm_service=advisory_reports_llm_service,
            config=config['advisory_reports']
        )
        
        # 8. Execute advisory reports processing
        logger.info("Starting advisory reports processing...")
        df_advisory_reports = advisory_reports_service.process_daily_adreports(
            date_bgn=date_bgn,
            date_end=date_end,
        )

        # 9. Save results
        if len(df_advisory_reports) > 0:
            advisory_reports_service.save_adreports_data(
                df_adreports = df_advisory_reports,
                date_end = date_end,
                output_dir = config['paths']['reports_outputs_dir']
            )    
        else:
            logger.warning("No valid advisory reports data")

        endtime = datetime.now()
        duration = endtime - start_time
        logger.info(f"Execution completed! Executed duration: {round(duration.total_seconds(), 2)} sec")
        
    except KeyboardInterrupt:
        logger.warning("Program interrupted by user")
        
    except Exception as e:
        logger.error(f"Program execution failed: {e}", exc_info=True)


if __name__ == "__main__":
    main()
