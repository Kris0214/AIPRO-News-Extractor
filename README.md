# AIPRO 新聞處理系統

自動化處理嘉實新聞資料，提取股票標的並生成摘要。

## 📁 專案結構

```
AIPRO_news/
├── config/
│   ├── config.yaml          # 主配置檔
│   ├── .env                 # 環境變數（帳密）
│   └── .env.example         # 環境變數範例
├── src/
│   ├── database.py          # 資料庫操作
│   ├── llm_service.py       # LLM 服務封裝
│   ├── news_service.py      # 新聞處理邏輯
│   └── utils.py             # 工具函數
├── src/




├── data/                    # 輸出資料目錄
├── logs/                    # 日誌目錄
├── main.py                  # 主程式入口
├── requirements.txt         # 依賴套件
└── README.md               # 本文件
```

## 🚀 快速開始

### 1. 環境設定

```powershell
# 建立虛擬環境
python -m venv .venv

# 啟動虛擬環境
.\.venv\Scripts\Activate.ps1

# 安裝依賴
pip install -r requirements.txt
```

### 2. 配置設定

複製環境變數範例檔並填入實際的帳號密碼：

```powershell
cp config\.env.example config\.env
```

編輯 `config\.env` 填入：
- `ODS_ACCOUNT`: Oracle 資料庫帳號
- `ODS_PASSWORD`: Oracle 資料庫密碼
- `AOAI_API_KEY`: Azure OpenAI API 金鑰

### 3. 執行程式

```powershell
python main.py
```

## 📝 配置說明

### config.yaml 主要配置項

- **database**: 資料庫連線設定
- **azure_openai**: Azure OpenAI API 設定
- **news**: 新聞處理參數
  - `num_workers`: 並行處理的線程數（預設 8）
  - `timeout`: 單個請求超時時間（預設 60 秒）
  - `excluded_keywords`: 要排除的關鍵字
  - `included_types`: 要包含的新聞類型

## 🔧 功能特色

- ✅ **自動化處理**: 每日自動擷取 T-1 日新聞資料
- ✅ **智能標籤**: 使用 GPT-4o 提取股票標的
- ✅ **自動摘要**: 生成 100-150 字新聞摘要
- ✅ **並行處理**: 多線程加速處理效率
- ✅ **錯誤重試**: 自動重試失敗的資料
- ✅ **日誌記錄**: 完整的執行日誌

## 📊 輸出格式

輸出檔案位於 `data/` 目錄，格式為 `嘉實新聞資料_YYYYMMDD.csv`

欄位說明：
- `snap_yyyymm`: 新聞日期
- `news`: 新聞內容
- `related_product`: 相關產品代碼
- `stock_desc`: 股票標的（格式：公司名(代碼)）
- `news_summary`: 新聞摘要

## 🛠️ 開發說明

### 擴展新功能

如需處理投顧報告，可以參考 `news_service.py` 建立 `report_service.py`：

```python
from src.report_service import ReportService

# 在 main.py 中使用
report_service = ReportService(db_manager, llm_service, config)
df_reports = report_service.process_daily_reports(date_bgn, date_end)
```

### 自訂 Prompt

修改 `src/llm_service.py` 中的 `extract_stock_info()` 和 `summarize_news()` 方法。


## 📄 授權

內部專案，僅供富邦內部使用。

