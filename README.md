# AIPRO News Extraction System

Automated system for processing financial news data, extracting stock targets, and generating summaries using Azure OpenAI GPT-4o.

## Project Structure

```
AIPRO-News-Extractor/
├── config/
│   ├── config.yaml          # Main configuration file
│   └── .env                 # Environment variables 
├── src/
│   ├── __init__.py
│   ├── database.py          # Database operations and connection management
│   ├── llm_service.py       # LLM service wrapper for Azure OpenAI
│   └── news_service.py      # News processing and parallel execution logic
├── utils/
│   ├── utils.py             # Utility functions (logging, config, date handling)
│   └── ...                  # Other utility scripts
├── prompts/
│   ├── extract_stock_target.txt      # Prompt template for stock extraction
│   ├── summarize_news.txt            # Prompt template for news summarization
│   ├── system_financial_tagger.txt   # System prompt for financial tagging
│   └── README.md                     # Prompt documentation
├── queries/
│   └── fetch_news.txt       # SQL query template for fetching news
├── instantclient/           # Oracle Instant Client (Depend on version you need)
├── data/                    # Output data directory
├── logs/                    # Log files directory
├── main.py                  # Main entry point
├── requirements.txt         # Python dependencies
└── README.md               # This file
```

## Quick Start

### 1. Environment Setup

```powershell
# Create virtual environment
python -m venv .venv

# Activate virtual environment (Windows PowerShell)
.\.venv\Scripts\Activate.ps1

# Install dependencies
pip install -r requirements.txt
```

### 2. Configuration

Create `.env` file in `config/` directory:

```bash
# config/.env
ODS_ACCOUNT=your_oracle_username
ODS_PASSWORD=your_oracle_password
AOAI_API_KEY=your_azure_openai_api_key
```

Edit `config/config.yaml` if needed to adjust:
- Database connection settings
- Azure OpenAI endpoint and model
- Processing parameters (workers, timeout)
- File paths and logging configuration

### 3. Run the System

```powershell
python main.py
```

## Configuration Guide

```yaml
# Database Configuration
database:
  host: "your-database-host"
  port: "5211"
  service_name: "YOUR_SERVICE"
  oracle_client_path: "./instantclient_23_9"

# Azure OpenAI Configuration
azure_openai:
  api_version: "2024-12-01-preview"
  endpoint: "https://your-endpoint.openai.azure.com/"
  model: "gpt-4o"
  max_tokens: 5000
  temperature: 0.1

# News Processing Configuration
news:
  num_workers: 8    # Number of parallel processing threads
  timeout: 60       # Request timeout in seconds

# Path Configuration
paths:
  outputs_dir: "./outputs"
  logs_dir: "./logs"

# Logging Configuration
logging:
  level: "INFO"
  format: "%(asctime)s - %(levelname)s - %(message)s"
  file_prefix: "aipro_news"
```

## Output Format

Output files are saved in `data/` directory with format: `嘉實新聞資料_YYYYMMDD.csv`

**Column Descriptions:**
- `snap_yyyymm`: News date (YYYYMMDD format)
- `news`: Full news content
- `related_product`: Related product codes
- `stock_desc`: Extracted stock targets (Format: CompanyName(StockCode))
- `news_summary`: Generated news summary (100-150 characters)


## Development Guide

### Architecture Overview

1. **main.py**: Orchestrates the entire workflow
2. **database.py**: Handles Oracle database connections with CLOB processing
3. **llm_service.py**: Wraps Azure OpenAI API calls with retry logic
4. **news_service.py**: Manages parallel news processing and data transformation
5. **utils.py**: Provides shared utilities (logging, config loading, date handling)

### Customizing Prompts

Edit prompt templates in `prompts/` directory:
- `extract_stock_target.txt`: Modify stock extraction logic
- `summarize_news.txt`: Adjust summarization style and length
- `system_financial_tagger.txt`: Change system behavior and constraints

### Customizing SQL Queries

Edit SQL templates in `queries/` directory:
- `fetch_news.txt`: Modify news data filtering and selection criteria


## Logging

Logs are stored in `logs/` directory with daily rotation:
- Format: `aipro_news_YYYYMMDD.log`
- Both console and file output
- Configurable log level in `config.yaml`

## License
Internal project for Fubon use only.




