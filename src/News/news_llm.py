"""
News-specific LLM Operations Module
Handles news-related LLM tasks (extraction, summarization)
"""
import logging
from pathlib import Path
from typing import Dict, Any

# Import utility functions
import sys
sys.path.insert(0, str(Path(__file__).parent.parent))
from utils.llm_service import LLMService
from utils.utils import load_prompt

logger = logging.getLogger(__name__)


class NewsLLMService:
    """News-specific LLM Service"""
    
    def __init__(self, llm_service: LLMService, prompts_dir: str = None):
        """
        Initialize news LLM service.
        
        Args:
            llm_service: Base LLM service instance
            prompts_dir: Directory containing prompt templates
        """
        self.llm = llm_service
        
        # Set prompts_dir relative to this file's location if not provided
        if prompts_dir is None:
            self.prompts_dir = str(Path(__file__).parent / "prompts")
        else:
            self.prompts_dir = prompts_dir
            
        logger.info(f"News LLM service initialized, prompts dir: {self.prompts_dir}")
    
    def extract_stock_info(self, news_text: str) -> str:
        """
        Extract the stock information from news text.

        Args:
            news_text: news text content
            
        Returns:
            symbol (format: company name(code))
        """
        system_content = load_prompt('system_financial_tagger', self.prompts_dir)
        json_schema = '{"股票標的": "string"}'
        
        user_prompt_template = load_prompt('extract_stock_target', self.prompts_dir)
        user_prompt = user_prompt_template.format(
            json_schema=json_schema,
            news_text=news_text
        )
        
        try:
            response = self.llm.call_with_json_schema(system_content, user_prompt)
            return response.get('股票標的', '無')
        except Exception as e:
            logger.warning(f"股票標的提取失敗: {e}")
            return "無"
    
    def summarize_news(self, news_text: str) -> str:
        """
        Generate news summary.
        
        Args:
            news_text: News text content
            
        Returns:
            News summary (100-150 characters)
        """
        system_content = load_prompt('system_financial_tagger', self.prompts_dir)
        json_schema = '{"新聞摘要": "string"}'
        
        user_prompt_template = load_prompt('summarize_news', self.prompts_dir)
        user_prompt = user_prompt_template.format(
            json_schema=json_schema,
            news_text=news_text
        )
        
        try:
            response = self.llm.call_with_json_schema(
                system_content, 
                user_prompt,
                max_tokens=500,
                temperature=1.0
            )
            return response.get('新聞摘要', '無')
        except Exception as e:
            logger.warning(f"News summarization failed: {e}")
            return "無"