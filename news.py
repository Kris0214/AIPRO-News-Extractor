import pandas as pd
import numpy as np
import os
import sys
sys.path.insert(0, r'D:\Kris\AIPRO_news\utils')
import glob
import time
from datetime import date, datetime, timedelta
import re
import logging
import argparse
import yaml
import warnings
import json
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
from openai import AzureOpenAI

from get_SQL_raw_data import get_SQL_raw_data, get_SQL_raw_data_clob 
from call_AOAI_api import call_AOAI_api_prompt 

warnings.filterwarnings("ignore")

## 載入設定檔
with open(r"D:\Kris\AIPRO_news\utils\config.yaml", 'r', encoding='utf-8') as f:
    config = yaml.safe_load(f)


# os.chdir(config['PROJECT_AIPRO_AI_DEV']['os_path'])


# 2. 設定時間參數
if date.today().weekday()==0:
    news_date_bgn = date.today() + timedelta(days=-3)
    news_date_end = date.today() + timedelta(days=-1)
    print('資料日期:', news_date_bgn, news_date_end)

else:
    news_date_bgn = date.today() + timedelta(days=-2)
    news_date_end = date.today() + timedelta(days=-1)
    print('資料日期:', news_date_bgn, news_date_end)

news_date_bgn = news_date_bgn.strftime("%Y/%m/%d")
news_date_end = news_date_end.strftime("%Y/%m/%d")

log_parameter = {'daliy_batch_news_date_bgn': news_date_bgn, 
                 'daliy_batch_news_date_end': news_date_end} 

with open('./log/log_parameter.yaml', 'w', encoding='utf-8') as f:
    yaml.safe_dump(log_parameter, f, allow_unicode=True)


# 5. 連線到 ODS 資料庫，檢核連線是否正常
account, pwd = config['PROJECT_AIPRO_AI_DEV']['ods_account'], config['PROJECT_AIPRO_AI_DEV']['ods_password']
try:
    get_SQL_raw_data(account = account, pwd = pwd, query = 'select sysdate from dual', ods_type = 'ods')
    print("程式檢核: ODS 連線正常!!!")

except:
    logging.info("程式檢核: ODS 連線失敗!!!")    


# 6. 連線到 Azure OpenAI 服務
client = AzureOpenAI(
    api_version = config['PROJECT_AIPRO_AI_DEV']['aoai_api_version'],
    azure_endpoint = config['PROJECT_AIPRO_AI_DEV']['aoai_endpoint'],
    api_key = config['PROJECT_AIPRO_AI_DEV']['aoai_api_key']
)

# %%


# %%
# 每日抓取嘉實新聞
query = f"""
SELECT NEWS_DATE,
      CONTENT AS NEWS_CONTENT,
      RELATED_PRODUCT
FROM dm_s_view.cwmdnews
WHERE NEWS_DATE between sysdate-2 AND sysdate-1
 AND RELATED_PRODUCT NOT LIKE '%NO300011%'
 AND RELATED_PRODUCT LIKE '%AS%'
 AND SUBJECT NOT LIKE '%經濟日報%'
 AND (
       NEWS_TYPE LIKE '%科技脈動%'
    OR NEWS_TYPE LIKE '%產業情報%'
    OR NEWS_TYPE LIKE '%國際股市%'
    OR NEWS_TYPE LIKE '%頭條新聞%'
    OR NEWS_TYPE LIKE '%研究報告%'
 )
"""
temp_news = get_SQL_raw_data_clob(account=account, pwd = pwd, query=query, ods_type='clob')

print('今日新聞資料，共:' + str(len(temp_news)) + '筆')
temp_news.head()

# for 測試用
# temp_news = temp_news.head()

# %%
def extract_stock_desc(args):
    """處理單個新聞項目的函數."""
    idx, news_text, client = args
    
    try:
        content = f"""你是一個金融文件標籤模型，負責將「新聞、投顧報告」轉換成固定json結構，請務必遵守json schema格式回覆，不可加入額外文字及註解。"""
        
        json_schema = """
            {
            "股票標的": "string"
            }
        """
        prompt_extract_stock_desc = \
            f"""嚴格遵守以下規格:依照以下json schema產出，確保可自動化執行，只回傳所需資訊。
                json schema規格如下:
                {json_schema}
                
                以下為新聞原文: + {str(news_text)} 
                1.股票標的:
                  -回傳格式限定於「公司行號 or 股票標的、4碼數字」，格式統一為「文字(4碼數字)」
                  -若報告提及多檔標的，只留一檔最主要的「公司行號 or 股票標的、4碼數字」
                  -若股票名稱中有股份有限公司直接刪除
                  -若未提及所需公司行號 or 股票標的，則顯示「無」 
             """
             
        
        response = call_AOAI_api_prompt(client = client, content = content, schema = json_schema, prompt = prompt_extract_stock_desc)
        response = json.loads(response)
        col_股票標的 = response['股票標的']

        return idx, news_text, col_股票標的
    
    except Exception as e:
        logging.warning(f"處理第 {idx} 筆新聞失敗: {str(e)}")
        return idx, news_text, "無"

# %%
def call_AOAI_api_news_summary(args):
    """處理單個新聞項目的函數."""
    idx, news_text, client = args

    try:
        json_schema = """
        {
        "新聞摘要": "string"
        }
        """
        response = client.chat.completions.create(
            messages=[
                {
                    "role": "system",
                    "content": "你是一個金融文件標籤模型，負責將「新聞、投顧報告」轉換成固定json結構，請務必遵守json schema格式回覆，不可加入額外文字及註解。",
                },
                {
                    "role": "user",
                    "content": f"""嚴格遵守以下規格:依照以下json schema產出，確保可自動化執行，只回傳所需資訊，字數約100字~150字。
                                   json schema規格如下:
                                   {json_schema}
                                   
                                   以下為新聞原文: + {str(news_text)}
                                   幫我自新聞原文進行摘要。"""
                }
            ],
            max_tokens=500,
            temperature=1.0,
            top_p=1.0,
            model = "gpt-4o",
            response_format={"type": "json_object"}
        )
        
        response = response.choices[0].message.content
        response = json.loads(response)
        col_新聞摘要 = response['新聞摘要']
        
        return idx, news_text, col_新聞摘要
    
    except Exception as e:
        logging.warning(f"處理第 {idx} 筆新聞失敗: {str(e)}")
        return idx, news_text, "無"

# %%
## 多線程處理 - 每日嘉實新聞批次貼標(股票代碼+新聞摘要) 
def process_news_parallel(type, df_source, client, num_workers=8):
    # 準備任務清單（支援傳入 Series 或 list）
    tasks = [(i, df_source.iloc[i], client) for i in range(len(df_source))]
    
    results = []
    
    with ThreadPoolExecutor(max_workers=num_workers) as executor:
        # 提交所有任務並建立 future->idx 映射
        submit_tasks = {executor.submit(type, task): task[0] for task in tasks}
        
        # 顯示進度條 (as_completed 會回傳完成的 Future)
        for submit_tasks in tqdm(as_completed(submit_tasks), total=len(tasks), desc="新聞處理進度"):
            try:
                idx, news_text, col_info = submit_tasks.result(timeout=60)  # 設定超時
                results.append((idx, news_text, col_info))
            
            except:
                pass
    
    # 按原始順序排序結果
    results.sort(key=lambda x: x[0])
    
    # 建立 DataFrame
    if results:
        _, news_text, col_info = zip(*results)
        df_news = pd.DataFrame({"news": news_text, "col_info": col_info})
    else:
        df_news = pd.DataFrame({"news": [], "col_info": []})
    
    return df_news

# %%
col_股票代碼 = \
process_news_parallel(type=extract_stock_desc, df_source=temp_news['news'], client=client, num_workers=8)

print(col_股票代碼.shape)
col_股票代碼.head()

# %%
col_新聞摘要 = \
process_news_parallel(type=call_AOAI_api_news_summary, df_source=temp_news['news'], client=client, num_workers=8)

print(col_新聞摘要.shape)
col_新聞摘要.head()

# %%
## 合併檔案
df_news = temp_news.merge( col_股票代碼, how = 'left', on = 'news')
df_news = df_news.merge( col_新聞摘要, how = 'left', on = 'news')

df_news.columns = ['snap_yyyymm', 'news', 'related_product', 'stock_desc', 'news_summary']
df_news

# %%
df_news_blank = pd.read_csv('./data_spec/嘉實新聞資料規格.csv')

save_filename = news_date_end[:4] + news_date_end[5:7] + news_date_end[8:]
pd.concat([df_news_blank, df_news[(df_news['stock_desc']!='無') | (df_news['news_summary']!='無')]], ignore_index=True).\
    to_csv(f'./data/嘉實新聞資料_{save_filename}.csv', index=False, encoding='utf-8-sig')

# %%
## 確認有無掉資料的...
df_news_untag = df_news[(df_news['stock_desc']=='無') | (df_news['news_summary']=='無')]
df_news_untag = df_news_untag[['snap_yyyymm', 'news', 'related_product']]
print('需要補標的資料筆數:', df_news_untag.shape)


if len(df_news_untag)>0:
    col_股票代碼 = \
    process_news_parallel(type=extract_stock_desc, df_source=df_news_untag['news'], client=client, num_workers=8)
    
    col_新聞摘要 = \
    process_news_parallel(type=call_AOAI_api_news_summary, df_source=df_news_untag['news'], client=client, num_workers=8)

    df_news = df_news_untag.merge( col_股票代碼, how = 'inner', on = 'news').\
                            merge( col_新聞摘要, how = 'inner', on = 'news')

    df_news.columns = ['snap_yyyymm', 'news', 'related_product', 'stock_desc', 'news_summary']
    
    df_news

else:
    df_news = df_news = pd.DataFrame(columns=['snap_yyyymm', 'news', 'related_product', 'stock_desc', 'news_summary'])

# %%
# 輸出final檔案: 嘉實新聞資料_yyyymmdd.csv
df_news_without_na = pd.read_csv(f'./data/嘉實新聞資料_{save_filename}.csv')
df_news_without_na = df_news_without_na[(df_news_without_na['stock_desc']!='無') & (df_news_without_na['news_summary']!='無')]

print(df_news_without_na.shape)

df_news_without_na = pd.concat([df_news_without_na, df_news], axis=0, ignore_index=True)
df_news_without_na.to_csv(f'./data/嘉實新聞資料_{save_filename}.csv', index=False, encoding='utf-8-sig')

print(df_news_without_na.shape)
