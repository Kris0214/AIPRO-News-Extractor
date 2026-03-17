# Prompt Templates

This directory contains prompt templates used by the LLM service for processing financial news data.

## Template Files

### 1. system_financial_tagger.txt

**Purpose**: System-level prompt that defines the model's role and behavior.

**Description**: 
- Sets up the LLM as a financial document tagging model
- Enforces strict JSON schema compliance
- Ensures structured output without additional commentary

**Usage**: Loaded as the system message for all LLM operations.

**Content**:
```
你是一個金融文件標籤模型，負責將「新聞、投顧報告」轉換成固定json結構，
請務必遵守json schema格式回覆，不可加入額外文字及註解。
```

---

### 2. extract_stock_target.txt

**Purpose**: Extract primary stock targets from financial news articles.

**Description**:
- Identifies the main company/stock mentioned in the news
- Extracts stock code (4-digit Taiwan stock code)
- Returns standardized format: `CompanyName(Code)`
- Handles multiple stocks by selecting only the primary target

**Template Variables**:
- `{json_schema}`: JSON schema definition for output structure
- `{news_text}`: The raw news article content

**Output Format**:
- Success: `台積電(2330)`
- No target found: `無`

**Rules**:
1. Only return the **most relevant** stock if multiple are mentioned
2. Remove "股份有限公司" from company names
3. Must use 4-digit Taiwan stock code
4. Format must be: `Name(Code)`
5. Return `無` if no valid stock target is found

**Example**:
```
Input: 台積電股份有限公司今日宣布...聯發科技也表示...
Output: 台積電(2330)
```

---

### 3. summarize_news.txt

**Purpose**: Generate concise summaries of financial news articles.

**Description**:
- Creates 100-150 character summaries in Traditional Chinese
- Maintains key financial information
- Returns JSON-structured output

**Template Variables**:
- `{json_schema}`: JSON schema definition for output structure
- `{news_text}`: The raw news article content

**Output Requirements**:
- Length: 100-150 characters
- Language: Traditional Chinese
- Content: Key facts and financial implications
- Format: JSON schema compliant

**Example**:
```
Input: [300-word article about TSMC earnings]
Output: 台積電第四季營收創新高，淨利年增35%，受惠於AI晶片需求強勁，
        公司上調全年資本支出至400億美元，看好先進製程需求持續成長。
```

---

## Usage in Code

### Loading Prompts

```python
from utils.utils import load_prompt

# Load system prompt
system_prompt = load_prompt('system_financial_tagger', './prompts')

# Load extraction prompt
extract_prompt = load_prompt('extract_stock_target', './prompts')

# Load summary prompt
summary_prompt = load_prompt('summarize_news', './prompts')
```

### Using in LLMService

```python
# In llm_service.py
def extract_stock_info(self, news_text: str) -> str:
    """Extract stock target from news"""
    user_prompt = load_prompt('extract_stock_target', self.prompts_dir)
    user_prompt = user_prompt.format(
        json_schema=self.json_schema,
        news_text=news_text
    )
    

def summarize_news(self, news_text: str) -> str:
    """Generate news summary"""
    user_prompt = load_prompt('summarize_news', self.prompts_dir)
    user_prompt = user_prompt.format(
        json_schema=self.json_schema,
        news_text=news_text
    )
    
```

---
