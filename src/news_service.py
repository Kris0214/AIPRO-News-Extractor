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

logger = logging.getLogger(__name__)


class NewsService:
    """News Processing Service"""
    
    def __init__(self, db_manager: Any, llm_service: Any, config: Dict[str, Any],
                 queries_dir: str = "./queries") -> None:
        """
        Initialize news service.
        
        Args:
            db_manager: Database manager instance
            llm_service: LLM service instance
            config: News configuration dictionary
            queries_dir: Directory containing SQL query templates
        """
        self.db = db_manager
        self.llm = llm_service
        self.config = config
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
        # 載入 SQL 模板
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
            return pd.DataFrame(columns=['snap_yyyymm', 'news', 'related_product'])
        
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
        # 處理日期格式
        if 'NEWS_DATE' in df_raw.columns:
            df_raw['snap_yyyymm'] = df_raw['NEWS_DATE'].apply(
                lambda x: x.strftime("%Y%m%d") if x is not None else None
            )
        
        # 重新命名欄位
        column_mapping = {
            'NEWS_CONTENT': 'news',
            'RELATED_PRODUCT': 'related_product'
        }
        df_raw = df_raw.rename(columns=column_mapping)
        
        # 選擇需要的欄位
        required_columns = ['snap_yyyymm', 'news', 'related_product']
        df_result = df_raw[required_columns].copy()
        
        return df_result
    
    def _process_single_news(self, args: Tuple[int, str, str]) -> Tuple[int, str, str]:
        """
        Process single news article (for parallel processing).
        
        Args:
            args: (idx, news_text, process_type)
            
        Returns:
            (idx, news_text, result)
        """
        idx, news_text, process_type = args
        
        try:
            if process_type == 'stock':
                result = self.llm.extract_stock_info(news_text)
            elif process_type == 'summary':
                result = self.llm.summarize_news(news_text)
            else:
                result = None
            
            return idx, news_text, result
            
        except Exception as e:
            logger.warning(f"Processing news {idx} failed ({process_type}): {e}")
            return idx, news_text, None
    
    def process_news_parallel(self, news_series: pd.Series, process_type: str) -> pd.DataFrame:
        """
        Process news in parallel (extract stocks or generate summaries).
        
        Args:
            news_series: News text Series
            process_type: Processing type ('stock' or 'summary')
            
        Returns:
            DataFrame containing news and col_info
        """
        desc_text = "Extract stock targets" if process_type == 'stock' else "Generate news summaries"
        logger.info(f"Starting {desc_text}, total {len(news_series)} articles")
        
        # Prepare tasks
        tasks = [(i, news_series.iloc[i], process_type) 
                for i in range(len(news_series))]
        
        results = []
        
        with ThreadPoolExecutor(max_workers=self.num_workers) as executor:
            futures = {executor.submit(self._process_single_news, task): task[0] 
                      for task in tasks}
            
            for future in tqdm(as_completed(futures), total=len(tasks), 
                             desc=desc_text):
                try:
                    idx, news_text, col_info = future.result(timeout=self.timeout)
                    results.append((idx, news_text, col_info))
                except Exception as e:
                    logger.error(f"Task execution failed: {e}")
        
        # Sort by index
        results.sort(key=lambda x: x[0])
        
        if results:
            _, news_texts, col_infos = zip(*results)
            df_result = pd.DataFrame({
                "news": news_texts,
                "col_info": col_infos
            })
        else:
            df_result = pd.DataFrame({"news": [], "col_info": []})
        
        logger.info(f"{desc_text} completed, successful {len(df_result)} articles")
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
        
        # 2. Extract stock targets
        df_stocks = self.process_news_parallel(df_news['news'], 'stock')
        
        # 3. Generate news summaries
        df_summaries = self.process_news_parallel(df_news['news'], 'summary')
        
        # 4. Merge results
        df_result = df_news.copy()
        df_result = df_result.merge(df_stocks, how='left', on='news')
        df_result = df_result.merge(df_summaries, how='left', on='news', 
                                   suffixes=('_stock', '_summary'))
        
        df_result.columns = ['snap_yyyymm', 'news', 'related_product', 
                            'stock_desc', 'news_summary']
        
        # 5. Handle missing data (retry)
        df_untag = df_result[
            (df_result['stock_desc'].isna()) | 
            (df_result['news_summary'].isna())
        ]
        
        if len(df_untag) > 0:
            logger.warning(f"Found {len(df_untag)} incomplete records, starting retry")
            
            df_retry_stocks = self.process_news_parallel(df_untag['news'], 'stock')
            df_retry_summaries = self.process_news_parallel(df_untag['news'], 'summary')
            
            df_retry = df_untag[['snap_yyyymm', 'news', 'related_product']].copy()
            df_retry = df_retry.merge(df_retry_stocks, how='left', on='news')
            df_retry = df_retry.merge(df_retry_summaries, how='left', on='news',
                                     suffixes=('_stock', '_summary'))
            df_retry.columns = ['snap_yyyymm', 'news', 'related_product',
                               'stock_desc', 'news_summary']
            
            # Update results (remove incomplete, add retry successes)
            df_result = df_result[
                (df_result['stock_desc'].notna()) & 
                (df_result['news_summary'].notna())
            ]
            df_result = pd.concat([df_result, df_retry], ignore_index=True)
        
        # 6. Final filter (keep valid data only)
        df_final = df_result[
            (df_result['stock_desc'].notna()) & 
            (df_result['news_summary'].notna())
        ]
        
        logger.info(f"Processing completed, final valid data: {len(df_final)} articles")

        
        return df_final
    
    def save_news_data(self, df_news: pd.DataFrame, date_end: str, 
                      output_dir: str = "./outputs") -> str:
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

