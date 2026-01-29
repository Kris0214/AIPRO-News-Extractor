"""
新聞處理服務模組
負責新聞資料的查詢、處理和標籤化
"""
import logging
import pandas as pd
from typing import List, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

logger = logging.getLogger(__name__)


class NewsService:
    """新聞處理服務"""
    
    def __init__(self, db_manager, llm_service, config: dict):
        """
        初始化新聞服務
        
        Args:
            db_manager: 資料庫管理器實例
            llm_service: LLM 服務實例
            config: 新聞配置
        """
        self.db = db_manager
        self.llm = llm_service
        self.config = config
        self.num_workers = config.get('num_workers', 8)
        self.timeout = config.get('timeout', 60)
        logger.info("新聞服務初始化完成")
    
    def build_news_query(self, date_bgn: str, date_end: str) -> str:
        """
        建立新聞查詢 SQL
        
        Args:
            date_bgn: 開始日期 (YYYY/MM/DD)
            date_end: 結束日期 (YYYY/MM/DD)
            
        Returns:
            SQL 查詢語句
        """
        excluded = self.config.get('excluded_keywords', [])
        included = self.config.get('included_types', [])
        
        # 建立排除條件
        exclude_conditions = "\n AND ".join([
            f"RELATED_PRODUCT NOT LIKE '%{kw}%'" for kw in excluded
        ])
        
        # 建立包含條件
        include_conditions = "\n    OR ".join([
            f"NEWS_TYPE LIKE '%{nt}%'" for nt in included
        ])
        
        query = f"""
SELECT NEWS_DATE,
       CONTENT AS NEWS_CONTENT,
       RELATED_PRODUCT
FROM dm_s_view.cwmdnews
WHERE NEWS_DATE BETWEEN TO_DATE('{date_bgn}', 'YYYY/MM/DD') 
                    AND TO_DATE('{date_end}', 'YYYY/MM/DD')
  AND {exclude_conditions}
  AND RELATED_PRODUCT LIKE '%AS%'
  AND SUBJECT NOT LIKE '%經濟日報%'
  AND (
       {include_conditions}
  )
"""
        return query
    
    def fetch_news(self, date_bgn: str, date_end: str) -> pd.DataFrame:
        """
        從資料庫擷取新聞資料並處理
        
        Args:
            date_bgn: 開始日期
            date_end: 結束日期
            
        Returns:
            新聞資料 DataFrame (欄位: snap_yyyymm, news, related_product)
        """
        query = self.build_news_query(date_bgn, date_end)
        logger.info(f"開始查詢新聞資料: {date_bgn} ~ {date_end}")
        
        # 使用通用的 fetch_dataframe 方法（自動處理 CLOB）
        df_raw = self.db.fetch_dataframe(query, process_clob=True)
        
        if len(df_raw) == 0:
            logger.warning("查詢結果為空")
            return pd.DataFrame(columns=['snap_yyyymm', 'news', 'related_product'])
        
        # 處理新聞資料格式（業務邏輯）
        df_news = self._process_news_data(df_raw)
        logger.info(f"查詢完成，共 {len(df_news)} 筆新聞")
        
        return df_news
    
    def _process_news_data(self, df_raw: pd.DataFrame) -> pd.DataFrame:
        """
        處理原始新聞資料（內部方法）
        
        Args:
            df_raw: 原始資料 DataFrame
            
        Returns:
            處理後的 DataFrame
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
    
    def _process_single_news(self, args: Tuple) -> Tuple[int, str, str]:
        """
        處理單筆新聞（用於並行處理）
        
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
                result = "無"
            
            return idx, news_text, result
            
        except Exception as e:
            logger.warning(f"處理第 {idx} 筆新聞失敗 ({process_type}): {e}")
            return idx, news_text, "無"
    
    def process_news_parallel(self, news_series: pd.Series, 
                             process_type: str) -> pd.DataFrame:
        """
        並行處理新聞（提取股票或生成摘要）
        
        Args:
            news_series: 新聞文本 Series
            process_type: 處理類型 ('stock' 或 'summary')
            
        Returns:
            包含 news 和 col_info 的 DataFrame
        """
        desc_text = "提取股票標的" if process_type == 'stock' else "生成新聞摘要"
        logger.info(f"開始{desc_text}，共 {len(news_series)} 筆")
        
        # 準備任務
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
                    logger.error(f"任務執行失敗: {e}")
        
        # 按原始順序排序
        results.sort(key=lambda x: x[0])
        
        if results:
            _, news_texts, col_infos = zip(*results)
            df_result = pd.DataFrame({
                "news": news_texts,
                "col_info": col_infos
            })
        else:
            df_result = pd.DataFrame({"news": [], "col_info": []})
        
        logger.info(f"{desc_text}完成，成功 {len(df_result)} 筆")
        return df_result
    
    def process_daily_news(self, date_bgn: str, date_end: str, 
                          output_dir: str = "./data") -> pd.DataFrame:
        """
        處理每日新聞的完整流程
        
        Args:
            date_bgn: 開始日期
            date_end: 結束日期
            output_dir: 輸出目錄
            
        Returns:
            處理完成的新聞 DataFrame
        """
        logger.info("=" * 60)
        logger.info("開始每日新聞處理流程")
        logger.info(f"日期範圍: {date_bgn} ~ {date_end}")
        logger.info("=" * 60)
        
        # 1. 擷取新聞
        df_news = self.fetch_news(date_bgn, date_end)
        
        if len(df_news) == 0:
            logger.warning("沒有新聞資料，結束處理")
            return pd.DataFrame()
        
        # 2. 提取股票標的
        df_stocks = self.process_news_parallel(df_news['news'], 'stock')
        
        # 3. 生成新聞摘要
        df_summaries = self.process_news_parallel(df_news['news'], 'summary')
        
        # 4. 合併結果
        df_result = df_news.copy()
        df_result = df_result.merge(df_stocks, how='left', on='news')
        df_result = df_result.merge(df_summaries, how='left', on='news', 
                                   suffixes=('_stock', '_summary'))
        
        df_result.columns = ['snap_yyyymm', 'news', 'related_product', 
                            'stock_desc', 'news_summary']
        
        # 5. 處理遺漏資料（重試）
        df_untag = df_result[
            (df_result['stock_desc'] == '無') | 
            (df_result['news_summary'] == '無')
        ]
        
        if len(df_untag) > 0:
            logger.warning(f"發現 {len(df_untag)} 筆未完成標籤的資料，開始重試")
            
            df_retry_stocks = self.process_news_parallel(df_untag['news'], 'stock')
            df_retry_summaries = self.process_news_parallel(df_untag['news'], 'summary')
            
            df_retry = df_untag[['snap_yyyymm', 'news', 'related_product']].copy()
            df_retry = df_retry.merge(df_retry_stocks, how='left', on='news')
            df_retry = df_retry.merge(df_retry_summaries, how='left', on='news',
                                     suffixes=('_stock', '_summary'))
            df_retry.columns = ['snap_yyyymm', 'news', 'related_product',
                               'stock_desc', 'news_summary']
            
            # 更新結果（移除未完成的，加入重試成功的）
            df_result = df_result[
                (df_result['stock_desc'] != '無') & 
                (df_result['news_summary'] != '無')
            ]
            df_result = pd.concat([df_result, df_retry], ignore_index=True)
        
        # 6. 最終過濾（只保留有效資料）
        df_final = df_result[
            (df_result['stock_desc'] != '無') & 
            (df_result['news_summary'] != '無')
        ]
        
        logger.info(f"處理完成，最終有效資料: {len(df_final)} 筆")
        logger.info("=" * 60)
        
        return df_final
    
    def save_news_data(self, df_news: pd.DataFrame, date_end: str, 
                      output_dir: str = "./data") -> str:
        """
        儲存新聞資料
        
        Args:
            df_news: 新聞 DataFrame
            date_end: 結束日期 (YYYY/MM/DD)
            output_dir: 輸出目錄
            
        Returns:
            儲存的檔案路徑
        """
        import os
        os.makedirs(output_dir, exist_ok=True)
        
        # 生成檔名
        filename = f"嘉實新聞資料_{date_end.replace('/', '')}.csv"
        filepath = os.path.join(output_dir, filename)
        
        # 儲存
        df_news.to_csv(filepath, index=False, encoding='utf-8-sig')
        logger.info(f"資料已儲存至: {filepath}")
        
        return filepath
