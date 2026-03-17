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


class AdReportsLLMService:
    """Advisory Reports-specific LLM Service"""
    
    def __init__(self, llm_service: LLMService, prompts_dir: str = None):
        """
        Initialize advisory reports LLM service.
        
        Args:
            llm_service: Base LLM service instance
            prompts_dir: Directory containing prompt templates
        """
        self.llm = llm_service
        self.client = llm_service.client
        
        # Set prompts_dir relative to this file's location if not provided
        if prompts_dir is None:
            self.prompts_dir = str(Path(__file__).parent / "prompts")
        else:
            self.prompts_dir = prompts_dir
            
        logger.info(f"Reports LLM service initialized, prompts dir: {self.prompts_dir}")
    
    def extract_reports(self, reports_text: str) -> str:
        """
        Generate reports summary.
        
        Args:
            reports_text: Reports text content
            
        Returns:
            Reports summary (100-150 characters)
        """
        system_content = load_prompt('system_prompt', self.prompts_dir)
        
        user_prompt_template = load_prompt('extract_reports', self.prompts_dir)
        user_prompt = user_prompt_template.format(
            reports_text=reports_text
        )
        
        try:
            response = self.llm.call_with_json_schema(
                system_content, 
                user_prompt,
                max_tokens=500,
                temperature=1.0
            )
            return response.get('報告摘要', '無')
        except Exception as e:
            logger.warning(f"Reports summarization failed: {e}")
            return "無"