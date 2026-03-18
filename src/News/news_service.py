"""
News Processing Service Module
Handles news data query, processing and tagging
"""
import logging
import pandas as pd
from pathlib import Path
from typing import List, Tuple, Dict, Any
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import sys
import os

# Import utility functions
sys.path.insert(0, str(Path(__file__).parent.parent))
from utils.utils import load_prompt
from src.News.news_llm import NewsLLMService

logger = logging.getLogger(__name__)


class NewsService:
    """News Processing Service"""
    
    def __init__(self, db_manager: Any, news_llm_service: NewsLLMService, config: Dict[str, Any],
                 queries_dir: str = None) -> None:
        """
        Initialize news service.
        
        Args:
            db_manager: Database manager instance
            llm_service: LLM service instance
            config: News configuration dictionary
            queries_dir: Directory containing SQL query templates
        """
        self.db = db_manager
        self.llm = news_llm_service
        self.config = config
        # Set queries_dir relative to this file's location if not provided
        if queries_dir is None:
            self.queries_dir = str(Path(__file__).parent / "queries")
        else:
            self.queries_dir = queries_dir
        self.num_workers = config.get('num_workers', 8)
        self.timeout = config.get('timeout', 60)
    
    def build_news_query(self, date_bgn: str, date_end: str) -> str:
        """
        Build news query SQL.
        
        Args:
            date_bgn: Start date (YYYY/MM/DD)
            date_end: End date (YYYY/MM/DD)
            
        Returns:
            SQL query statement
        """
        query_template = load_prompt('fetch_news', self.queries_dir)
        query = query_template.format(
            date_bgn=date_bgn,
            date_end=date_end
        )
        
        return query
    
    def fetch_news(self, date_bgn: str, date_end: str) -> pd.DataFrame:
        """
        Fetch news data from database and process.
        
        Args:
            date_bgn: Start date
            date_end: End date
            
        Returns:
            News data DataFrame (columns: snap_yyyymm, news, related_product)
        """
        query = self.build_news_query(date_bgn, date_end)
        logger.info(f"Starting news query: {date_bgn} ~ {date_end}")
        
        df_raw = self.db.fetch_dataframe(query, process_clob=True)
        
        if len(df_raw) == 0:
            logger.warning("Query result is empty")
            return pd.DataFrame(columns=['NEWS_DATE', 'NEWS_CONTENT', 'RELATED_PRODUCT'])
        
        # Process news data format
        df_news = self._process_news_data(df_raw)
        logger.info(f"Query completed, {len(df_news)} news articles")
        
        return df_news
    
    def _process_news_data(self, df_raw: pd.DataFrame) -> pd.DataFrame:
        """
        Process raw news data (internal method).
        
        Args:
            df_raw: Raw data DataFrame
            
        Returns:
            Processed DataFrame
        """
        if 'NEWS_DATE' in df_raw.columns:
            df_raw['SNAP_DATE'] = df_raw['NEWS_DATE'].apply(
                lambda x: x.strftime("%Y/%m/%d") if x is not None else None
            )
            df_raw['SNAP_YYYYMM'] = df_raw['NEWS_DATE'].apply(
                lambda x: x.strftime("%Y%m") if x is not None else None
            )

        required_columns = ['SNAP_DATE', 'SNAP_YYYYMM', 'NEWS_CONTENT', 'RELATED_PRODUCT']
        df_result = df_raw[required_columns].copy()
        
        return df_result
    
    def _process_single_news(self, args: Tuple[int, str]) -> Tuple[int, str, dict]:
        """
        Process single news article (for parallel processing).
        
        Args:
            args: (idx, news_text)
            
        Returns:
            (idx, news_text, result_dict)
        """
        idx, news_text = args
        
        try:
            result = self.llm.summarize_news(news_text)
            return idx, news_text, result
            
        except Exception as e:
            logger.warning(f"Processing news {idx} failed: {e}")
            return idx, news_text, None
    
    def process_news_parallel(self, news_series: pd.Series) -> pd.DataFrame:
        """
        Process news in parallel (extract stock info, summary and labels in one call).
        
        Args:
            news_series: News text Series
            
        Returns:
            DataFrame containing news, PROD_ABBR_NAME, PROD_CODE, NEWS_SUMMARY, LABELS
        """
        logger.info(f"Starting news processing, total {len(news_series)} articles")
        
        # Prepare tasks
        tasks = [(i, news_series.iloc[i]) for i in range(len(news_series))]
        
        results = []
        
        with ThreadPoolExecutor(max_workers=self.num_workers) as executor:
            futures = {executor.submit(self._process_single_news, task): task[0] 
                      for task in tasks}
            
            for future in tqdm(as_completed(futures), total=len(tasks),
                             desc="Processing news"):
                try:
                    idx, news_text, result = future.result(timeout=self.timeout)
                    results.append((idx, news_text, result))
                except Exception as e:
                    logger.error(f"Task execution failed: {e}")
        
        # Sort by index
        results.sort(key=lambda x: x[0])
        
        if results:
            _, news_texts, result_dicts = zip(*results)
            df_result = pd.DataFrame(result_dicts)
        else:
            df_result = pd.DataFrame(columns=['PROD_ABBR_NAME', 'PROD_CODE', 'NEWS_SUMMARY', 'LABELS'])
        
        logger.info(f"News processing completed, {len(df_result)} articles")
        return df_result
    
    def process_daily_news(self, date_bgn: str, date_end: str) -> pd.DataFrame:
        """
        Complete workflow for processing daily news.
        
        Args:
            date_bgn: Start date
            date_end: End date
            
        Returns:
            Processed news DataFrame
        """
        
        # 1. Fetch news
        df_news = self.fetch_news(date_bgn, date_end)
        
        if len(df_news) == 0:
            logger.warning("No news data, ending processing")
            return pd.DataFrame()
        
        # 2. Process all news (stock info + summary + labels in one call)
        df_processed = self.process_news_parallel(df_news['NEWS_CONTENT'])
        
        # 3. Assign results (index is aligned)
        llm_cols = ['PROD_ABBR_NAME', 'PROD_CODE', 'NEWS_SUMMARY', 'LABELS']
        df_result = df_news.reset_index(drop=True).copy()
        df_result[llm_cols] = df_processed[llm_cols]
        
        # 4. Handle missing data (retry)
        retry_mask = df_result['NEWS_SUMMARY'].isna()
        if retry_mask.any():
            logger.warning(f"Found {retry_mask.sum()} incomplete records, starting retry")
            df_retry_processed = self.process_news_parallel(df_result.loc[retry_mask, 'NEWS_CONTENT'])
            df_result.loc[retry_mask, llm_cols] = df_retry_processed[llm_cols].values
        
        # 5. Final filter (keep valid data only)
        df_final = df_result[df_result['NEWS_SUMMARY'].notna()]
        
        logger.info(f"Processing completed, final valid data: {len(df_final)} articles")

        
        return df_final
    
    def save_news_data(self, df_news: pd.DataFrame, date_end: str, 
                      output_dir: str = "./outputs/新聞資料") -> str:
        """
        Save news data.
        
        Args:
            df_news: News DataFrame
            date_end: End date (YYYY/MM/DD)
            output_dir: Output directory
            
        Returns:
            Saved file path
        """
        os.makedirs(output_dir, exist_ok=True)
        
        # Generate filename
        filename = f"嘉實新聞資料_{date_end.replace('/', '')}.csv"
        filepath = os.path.join(output_dir, filename)
        
        # Save
        df_news.to_csv(filepath, index=False, encoding='utf-8-sig')
        logger.info(f"Data saved to: {filepath}")
        
        return filepath

