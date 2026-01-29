import pandas as pd
import numpy as np
import os
import glob
import time
import win32com.client
from docx import Document
from pypandoc import convert_file

def select_emails(filter_rules):
    outlook = win32com.client.Dispatch("Outlook.Application").GetNamespace("MAPI")
    inbox = outlook.GetDefaultFolder(6)  # 6 代表收件匣 (olFolderInbox)
    messages = inbox.Items

    for filter_rule in filter_rules:
        messages = messages.Restrict(filter_rule)
    
    for message in messages:
        print(f"Subject: {message.Subject}")
        print(f"ReceivedTime: {message.ReceivedTime}")

    return messages



def download_attachments(messages, download_folder):
    for message in messages:
        print(message.Subject)

        for attachment in message.attachments:
            print(attachment.FileName)
            attachment.SaveAsFile( os.path.abspath(download_folder) + '\\' +  attachment.FileName)



def extract_text_from_docx(path, type = 'plain_text'):
    ## extract_text_from_docx output
    full_text = []

    if type == 'plain_text':
        for i in os.listdir(path):
            print(path + '/' + i)
            doc = Document(path + '/' + i)
            
            text = ""
            for para in doc.paragraphs:
                # print(para.text)
                text = text + para.text

            full_text.append(text)
    
    else:
        for i in os.listdir(path):
            print(path + '/' + i)
            md = convert_file(path + '/' + i, "md", format = "docx",
                        extra_args=["--standalone", "--wrap=none"])   
            full_text.append(md)

    return full_text



def send_email_via_outlook(subject, body, email, attachment_path):
    try:
        outlook = win32com.client.Dispatch("Outlook.Application")
        mail = outlook.CreateItem(0)  # 0 代表 olMailItem

        mail.Subject = subject
        mail.Body = body
        mail.To = email

        for attachment in  attachment_path:
            mail.Attachments.Add(attachment)

        mail.Send()
        print(f"郵件已發送: {email}")

    except Exception as e:
        print(f"郵件發送錯誤: {e}")