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


    def summarize_news(self, news_text: str) -> dict:
        """
        Extract stock info, generate news summary, and produce labels.
        
        Args:
            news_text: News text content
            
        Returns:
            dict with keys: PROD_ABBR_NAME, PROD_CODE, NEWS_SUMMARY, LABELS
        """
        system_content = load_prompt('system_financial_tagger', self.prompts_dir)
        json_schema = '{"PROD_ABBR_NAME": "string", "PROD_CODE":"string", "NEWS_SUMMARY":"string","LABELS":"array"}'
        
        user_prompt_template = load_prompt('summarize_news', self.prompts_dir)
        user_prompt = user_prompt_template.replace('{json_schema}', json_schema).replace('{news_text}', news_text)
        
        try:
            response = self.llm.call_with_json_schema(
                system_content, 
                user_prompt,
                max_tokens=2000,
                temperature=1.0
            )
            if response is None:
                return {"PROD_ABBR_NAME": None, "PROD_CODE": None, "NEWS_SUMMARY": None, "LABELS": None}
            return response
        except Exception as e:
            logger.warning(f"News summarization failed: {e}")
            return {"PROD_ABBR_NAME": None, "PROD_CODE": None, "NEWS_SUMMARY": None, "LABELS": None}