"""
News Processing Service Module
Handles news data query, processing and tagging
"""
from datetime import date, timedelta
import logging
from unittest import result
from openai import AzureOpenAI
import pandas as pd
from pathlib import Path
from typing import List, Tuple, Dict, Any
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import sys
import os
import json
import requests

import win32com.client
from docx import Document
from pypandoc import convert_file

# Import utility functions
sys.path.insert(0, str(Path(__file__).parent.parent))
from utils.utils import load_prompt
from src.Advisory_reports.adreports_llm import AdReportsLLMService  # ← 新增

logger = logging.getLogger(__name__)


class AdReports_process:
    """Advisory Reports Processing Service"""
    
    def __init__(self, db_manager: Any, advisory_reports_llm_service: AdReportsLLMService, config: Dict[str, Any]) -> None:
        """
        Initialize advisory reports service.
        
        Args:
            db_manager: Database manager instance
            llm_service: LLM service instance
            config: News configuration dictionary
            queries_dir: Directory containing SQL query templates
        """
        self.db = db_manager
        self.client = advisory_reports_llm_service.client
        self.config = config
        # Set queries_dir relative to this file's location if not provided
        self.queries_dir = str(Path(__file__).parent / "queries")
        self.subject_keyword = config.get('subject_keyword', '')
        self.num_workers = config.get('num_workers', 8)
        self.timeout = config.get('timeout', 60)

        prompts_dir = Path(__file__).parent / "prompts"
        with open(prompts_dir / "system_prompt.txt", 'r', encoding='utf-8') as f:
            self.system_prompt = f.read()
        with open(prompts_dir / "extract_reports.txt", 'r', encoding='utf-8') as f:
            self.content_prompt = f.read()
    
    def _extract_text_from_docx(self, path: str, type = 'plain_text'):
        ## extract_text_from_docx output
        full_text = []

        if type == 'plain_text':
            for i in os.listdir(path):
                print(path + '/' + i)
                doc = Document(path + '/' + i)
                
                text = ""
                for para in doc.paragraphs:
                    text = text + para.text

                full_text.append(text)

        else:
            for i in os.listdir(path):
                print(path + '/' + i)
                md = convert_file(path + '/' + i, "md", format = "docx",
                            extra_args=["--standalone", "--wrap=none"])   
                full_text.append(md)

        return full_text
    
    def _call_AOAI_api_report_summary(self, args):
        """處理單個投顧報告項目的函數."""
        idx, report_text, client = args

        system_prompt = self.system_prompt
        content_prompt = self.content_prompt
        user_content = content_prompt.replace("{report_text}", report_text)
        
        logger.debug(f"Processing report {idx}")

        try:
            response = client.chat.completions.create(
                messages=[
                    { "role": "system", "content": system_prompt, },
                    { "role": "user", "content": user_content }
                ],
                max_tokens=self.config.get('max_tokens', 5000),
                temperature=self.config.get('temperature', 0.5),
                top_p=1.0,
                model = self.config.get('model', "gpt-4o"),
                response_format={"type": "json_object"}
            )
            
            results = json.loads(response.choices[0].message.content)

            return idx, report_text, {
                key: results.get(key, "無")
                for key in [
                    'PROD_ABBR_NAME', 'PROD_CODE', 'HOLDING_SUGGEST', 'TARGET_PRICE',
                    'EPS_ESTIMATE', 'HOUSE_VIEW_MEMBER', 'HOUSE_VIEW_PUBLIC'
                ]
            }

        except Exception as e:
            logger.warning(f"處理第 {idx} 筆報告失敗: {str(e)}")
            return idx, report_text, {
                key: "無"
                for key in [
                    'PROD_ABBR_NAME', 'PROD_CODE', 'HOLDING_SUGGEST', 'TARGET_PRICE',
                    'EPS_ESTIMATE', 'HOUSE_VIEW_MEMBER', 'HOUSE_VIEW_PUBLIC'
                ]
            }
    
    def _process_adreports_parallel(self, Tool, reports, client):
        # 準備任務清單（支援傳入 Series 或 list）
        tasks = [(i, reports[i], client) for i in range(len(reports))]
        
        results = []
        
        with ThreadPoolExecutor(max_workers = self.num_workers) as executor:
            # 提交所有任務並建立 future->idx 映射
            submit_tasks = {executor.submit(Tool, task): task[0] for task in tasks}
            
            # 顯示進度條 (as_completed 會回傳完成的 Future)
            for submit_task in tqdm(as_completed(submit_tasks), total=len(tasks), desc="投顧報告處理進度"):
                idx = submit_tasks[submit_task]
                try:
                    idx, report_text, col_info = submit_task.result(timeout=self.timeout)
                    results.append((idx, report_text, col_info))
                
                except Exception as e:
                    logger.warning(f"處理第 {idx} 筆報告失敗: {str(e)}")
                    pass
        
        # 按原始順序排序結果
        results.sort(key=lambda x: x[0])
        
        # 建立 DataFrame
        if results:
            _, report_text, col_info = zip(*results)
            reports_summaries = pd.DataFrame(col_info)
            reports_summaries.insert(0, "house_view_report", report_text)
        else:
            reports_summaries = pd.DataFrame()

        return reports_summaries
    
    def process_daily_adreports(self, date_bgn: str, date_end: str) -> pd.DataFrame:
        """
        Complete workflow for processing daily news.
        
        Args:
            date_bgn: Start date
            date_end: End date
            
        Returns:
            Processed news DataFrame
        """

        # Request PowerAutomate flow to get advisory reports data 
        response = requests.get(url=self.config['API_URL'])
        logger.info(f"PowerAutomate flow response: {response.status_code}, {response.text}")

        # 1. load advisory reports from files (use yesterday)
        today = date.today().strftime('%Y%m%d')
        reports_texts = self._extract_text_from_docx(
            str(Path(__file__).parent.parent.parent / "data" / "Fubon Research" / today),
            type='plain_text'
        )

        if len(reports_texts) == 0:
            logger.warning("No advisory reports, ending processing")
            return pd.DataFrame()

        reports_summaries = self._process_adreports_parallel(
                                    Tool = self._call_AOAI_api_report_summary
                                    , reports = reports_texts
                                    , client = self.client
                                )
        
        reports_summaries['SNAP_DATE'] = date_end
        reports_summaries['SNAP_YYYYMM'] = date_end.replace('/', '')[:6]
        
        df_result = reports_summaries[['SNAP_DATE', 'SNAP_YYYYMM', 'PROD_ABBR_NAME', 'PROD_CODE',
                                       'HOUSE_VIEW_PUBLIC', 'HOUSE_VIEW_MEMBER',
                                       'TARGET_PRICE', 'HOLDING_SUGGEST', 'EPS_ESTIMATE']]
        
        logger.info(f"Processing completed, final valid data: {len(df_result)} articles")
        return df_result
    
    def save_adreports_data(self, df_adreports: pd.DataFrame, date_end: str, 
                            output_dir: str = "./outputs/投顧報告摘要") -> str:
        """
        Save advisory reports data.
        
        cols : snap_yyyymm,stock_desc,prospect_for_public,prospect_for_member,target_price,holding_suggest,eps
        Args:
            df_adreports: Advisory reports DataFrame
            date_end: End date (YYYY/MM/DD)
            output_dir: Output directory
            
        Returns:
            Saved file path
        """
        os.makedirs(output_dir, exist_ok=True)
        
        # Generate filename
        filename = f"投顧報告摘要_{date_end.replace('/', '')}.csv"
        filepath = os.path.join(output_dir, filename)
        
        # Save
        df_adreports.to_csv(filepath, index=False, encoding='utf-8-sig')
        logger.info(f"Data saved to: {filepath}")
        
        return filepath


