"""
LLM 服務模組
封裝 Azure OpenAI API 調用
"""
import json
import logging
from typing import Dict, Any, Optional
from openai import AzureOpenAI

logger = logging.getLogger(__name__)


class LLMService:
    """Azure OpenAI 服務封裝"""
    
    def __init__(self, endpoint: str, api_key: str, api_version: str, 
                 model: str, max_tokens: int = 5000, temperature: float = 0.1):
        """
        初始化 LLM 服務
        
        Args:
            endpoint: Azure OpenAI 端點
            api_key: API 金鑰
            api_version: API 版本
            model: 模型名稱
            max_tokens: 最大 token 數
            temperature: 溫度參數
        """
        self.client = AzureOpenAI(
            api_version=api_version,
            azure_endpoint=endpoint,
            api_key=api_key
        )
        self.model = model
        self.max_tokens = max_tokens
        self.temperature = temperature
        logger.info(f"LLM 服務初始化完成，模型: {model}")
    
    def call_with_json_schema(self, system_content: str, user_prompt: str, 
                             max_tokens: Optional[int] = None,
                             temperature: Optional[float] = None) -> Dict[str, Any]:
        """
        調用 LLM 並返回 JSON 格式回應
        
        Args:
            system_content: 系統提示詞
            user_prompt: 使用者提示詞
            max_tokens: 覆寫預設的最大 token 數
            temperature: 覆寫預設的溫度參數
            
        Returns:
            解析後的 JSON 回應
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
                response_format={"type": "json_object"}
            )
            
            content = response.choices[0].message.content
            return json.loads(content)
            
        except json.JSONDecodeError as e:
            logger.error(f"JSON 解析失敗: {e}")
            raise
        except Exception as e:
            logger.error(f"LLM 調用失敗: {e}")
            raise
    
    def extract_stock_info(self, news_text: str) -> str:
        """
        從新聞文本中提取股票標的資訊
        
        Args:
            news_text: 新聞文本
            
        Returns:
            股票標的（格式: 公司名(代碼)）
        """
        system_content = """你是一個金融文件標籤模型，負責將「新聞、投顧報告」轉換成固定json結構，請務必遵守json schema格式回覆，不可加入額外文字及註解。"""
        
        json_schema = '{"股票標的": "string"}'
        
        user_prompt = f"""嚴格遵守以下規格:依照以下json schema產出，確保可自動化執行，只回傳所需資訊。
json schema規格如下:
{json_schema}

以下為新聞原文: {news_text}

1.股票標的:
  -回傳格式限定於「公司行號 or 股票標的、4碼數字」，格式統一為「文字(4碼數字)」
  -若報告提及多檔標的，只留一檔最主要的「公司行號 or 股票標的、4碼數字」
  -若股票名稱中有股份有限公司直接刪除
  -若未提及所需公司行號 or 股票標的，則顯示「無」
"""
        
        try:
            response = self.call_with_json_schema(system_content, user_prompt)
            return response.get('股票標的', '無')
        except Exception as e:
            logger.warning(f"股票標的提取失敗: {e}")
            return "無"
    
    def summarize_news(self, news_text: str) -> str:
        """
        生成新聞摘要
        
        Args:
            news_text: 新聞文本
            
        Returns:
            新聞摘要（100-150字）
        """
        system_content = """你是一個金融文件標籤模型，負責將「新聞、投顧報告」轉換成固定json結構，請務必遵守json schema格式回覆，不可加入額外文字及註解。"""
        
        json_schema = '{"新聞摘要": "string"}'
        
        user_prompt = f"""嚴格遵守以下規格:依照以下json schema產出，確保可自動化執行，只回傳所需資訊，字數約100字~150字。
json schema規格如下:
{json_schema}

以下為新聞原文: {news_text}
幫我自新聞原文進行摘要。"""
        
        try:
            response = self.call_with_json_schema(
                system_content, 
                user_prompt,
                max_tokens=500,
                temperature=1.0
            )
            return response.get('新聞摘要', '無')
        except Exception as e:
            logger.warning(f"新聞摘要生成失敗: {e}")
            return "無"
