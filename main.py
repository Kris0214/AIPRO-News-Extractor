"""
AIPRO 新聞處理系統 - 主程式
每日自動化處理新聞資料，提取股票標的並生成摘要
"""
import os
import sys
import logging
from pathlib import Path

# 加入 src 目錄到路徑
sys.path.insert(0, str(Path(__file__).parent / "src"))

from src import llm_service
from src.database import DatabaseManager
from src.llm_service import LLMService
from src.news_service import NewsService
from utils.utils import (
    setup_logging, load_config, load_env_variables,
    get_date_range, save_log_parameter, ensure_directories
)

logger = logging.getLogger(__name__)


def main():
    """主程式入口"""
    try:
        # 1. 載入配置
        config = load_config("./config/config.yaml")
        load_env_variables("./config/.env")
        
        # 2. 設定日誌（必須先設定才能使用 logger）
        setup_logging(
            config['logging'],
            config['paths']['logs_dir']
        )
        
        logger.info("=" * 80)
        logger.info("AIPRO 新聞處理系統啟動")
        logger.info("=" * 80)
        
        # 3. 確保必要目錄存在
        ensure_directories(
            config['paths']['data_dir'],
            config['paths']['logs_dir']
        )
        
        # 4. 獲取日期範圍
        date_bgn, date_end = get_date_range()
        logger.info(f"處理日期範圍: {date_bgn} ~ {date_end}")
        
        # 儲存執行參數
        save_log_parameter(date_bgn, date_end)
        
        # 5. 初始化資料庫管理器
        logger.info("初始化資料庫連線...")
        db_manager = DatabaseManager(
            account=os.getenv('ODS_ACCOUNT'),
            password=os.getenv('ODS_PASSWORD'),
            host=config['database']['host'],
            port=config['database']['port'],
            service_name=config['database']['service_name'],
            oracle_client_path=config['database']['oracle_client_path']
        )
        

        query = f"""
        SELECT NEWS_DATE,
            CONTENT AS NEWS_CONTENT,
            RELATED_PRODUCT
        FROM dm_s_view.cwmdnews
        WHERE NEWS_DATE between sysdate-2 AND sysdate-1
        AND RELATED_PRODUCT NOT LIKE '%NO300011%'
        AND RELATED_PRODUCT LIKE '%AS%'
        AND SUBJECT NOT LIKE '%經濟日報%'
        AND (
            NEWS_TYPE LIKE '%科技脈動%'
            OR NEWS_TYPE LIKE '%產業情報%'
            OR NEWS_TYPE LIKE '%國際股市%'
            OR NEWS_TYPE LIKE '%頭條新聞%'
            OR NEWS_TYPE LIKE '%研究報告%'
        )
        """
        df_news = db_manager.fetch_dataframe(query)
        print(df_news)

        logger.info("✅ 資料庫連線測試成功")
        return 0
        
        # 測試資料庫連線
        # if not db_manager.test_connection():
        #     logger.error("資料庫連線失敗，程式終止")
        #     return 1
        
        # # 6. 初始化 LLM 服務
        # logger.info("初始化 LLM 服務...")
        # llm_service = LLMService(
        #     endpoint=config['azure_openai']['endpoint'],
        #     api_key=os.getenv('AOAI_API_KEY'),
        #     api_version=config['azure_openai']['api_version'],
        #     model=config['azure_openai']['model'],
        #     max_tokens=config['azure_openai']['max_tokens'],
        #     temperature=config['azure_openai']['temperature']
        # )
        
        # # 7. 初始化新聞服務
        # logger.info("初始化新聞服務...")
        # news_service = NewsService(
        #     db_manager=db_manager,
        #     llm_service=llm_service,
        #     config=config['news']
        # )
        
        # # 8. 執行新聞處理
        # logger.info("開始處理新聞...")
        # df_news = news_service.process_daily_news(
        #     date_bgn=date_bgn,
        #     date_end=date_end,
        #     output_dir=config['paths']['data_dir']
        # )
        
    #     # 9. 儲存結果
    #     if len(df_news) > 0:
    #         output_file = news_service.save_news_data(
    #             df_news=df_news,
    #             date_end=date_end,
    #             output_dir=config['paths']['data_dir']
    #         )
    #         logger.info(f"✅ 處理完成！共 {len(df_news)} 筆新聞")
    #         logger.info(f"📁 輸出檔案: {output_file}")
    #     else:
    #         logger.warning("⚠️  沒有有效的新聞資料")
        
    #     logger.info("=" * 80)
    #     logger.info("程式執行完成")
    #     logger.info("=" * 80)
        
    #     return 0
        
    except KeyboardInterrupt:
        logger.warning("程式被使用者中斷")
        return 130
        
    except Exception as e:
        logger.error(f"程式執行失敗: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())
# 