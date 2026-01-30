"""
LLM Service Module
Encapsulates Azure OpenAI API calls
"""
import json
import logging
import os
from pathlib import Path
from typing import Dict, Any, Optional
from openai import AzureOpenAI

# Import utility functions
import sys
sys.path.insert(0, str(Path(__file__).parent.parent))
from utils.utils import load_prompt

logger = logging.getLogger(__name__)


class LLMService:
    """Azure OpenAI Service Wrapper"""
    
    def __init__(self, endpoint: str, api_key: str, api_version: str, 
                 model: str, max_tokens: int = 5000, temperature: float = 0.1,
                 timeout: int = 60, prompts_dir: str = "./prompts") -> None:
        """
        Initialize LLM service.
        
        Args:
            endpoint: Azure OpenAI endpoint
            api_key: API key
            api_version: API version
            model: Model name
            max_tokens: Maximum number of tokens
            temperature: Temperature parameter
            timeout: Request timeout in seconds
            prompts_dir: Directory containing prompt templates
        """
        self.client = AzureOpenAI(
            api_version=api_version,
            azure_endpoint=endpoint,
            api_key=api_key,
            timeout=timeout
        )
        self.model = model
        self.max_tokens = max_tokens
        self.temperature = temperature
        self.timeout = timeout
        self.prompts_dir = prompts_dir
        logger.info(f"LLM service initialized, model: {model}, timeout: {timeout}s")
    
    def call_with_json_schema(self, system_content: str, user_prompt: str, 
                             max_tokens: Optional[int] = None,
                             temperature: Optional[float] = None) -> Dict[str, Any]:
        """
        Call LLM and return JSON formatted response.
        
        Args:
            system_content: System prompt
            user_prompt: User prompt
            max_tokens: Override default max tokens
            temperature: Override default temperature
            
        Returns:
            Parsed JSON response
        """
        try:
            response = self.client.chat.completions.create(
                messages=[
                    {"role": "system", "content": system_content},
                    {"role": "user", "content": user_prompt}
                ],
                max_tokens=max_tokens or self.max_tokens,
                temperature=temperature or self.temperature,
                top_p=1.0,
                model=self.model,
                response_format={"type": "json_object"},
                timeout=self.timeout
            )
            
            content = response.choices[0].message.content
            if content is None:
                raise ValueError("LLM returned empty content")
            return json.loads(content)
            
        except json.JSONDecodeError as e:
            logger.error(f"JSON parsing failed: {e}")
            raise
        except Exception as e:
            logger.error(f"LLM call failed: {e}")
            raise
    
    def extract_stock_info(self, news_text: str) -> str:
        """
        Extract the stock information from news text.

        Args:
            news_text: news text content
            
        Returns:
            symbol（format: company name(code)）
        """
        system_content = load_prompt('system_financial_tagger', self.prompts_dir)
        json_schema = '{"股票標的": "string"}'
        
        user_prompt_template = load_prompt('extract_stock_target', self.prompts_dir)
        user_prompt = user_prompt_template.format(
            json_schema=json_schema,
            news_text=news_text
        )
        
        try:
            response = self.call_with_json_schema(system_content, user_prompt)
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
            response = self.call_with_json_schema(
                system_content, 
                user_prompt,
                max_tokens=500,
                temperature=1.0
            )
            return response.get('新聞摘要', '無')
        except Exception as e:
            logger.warning(f"News summarization failed: {e}")
            return "無"
