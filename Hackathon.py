# Databricks notebook source
import os
import requests
import base64

# COMMAND ----------

!pip3.10 install openai==1.13.3

# COMMAND ----------

!pip install --upgrade pip

# COMMAND ----------

!export AZURE_OPENAI_API_KEY="c686ebee29804e5a85d650ac50b6cf84" 
!export AZURE_OPENAI_ENDPOINT="https://everybodyhackon.openai.azure.com/" 


# COMMAND ----------

AZURE_OPENAI_API_KEY="c686ebee29804e5a85d650ac50b6cf84" 
AZURE_OPENAI_ENDPOINT="https://everybodyhackon.openai.azure.com/" 

# COMMAND ----------

import openai

# COMMAND ----------

from openai import AzureOpenAI

# COMMAND ----------

!python --version

# COMMAND ----------

/Workspace/Users/pallavi.samodia@razorpay.com/Razorpay_payments-1170x508.png

# COMMAND ----------

# Configuration
GPT4V_KEY = "c686ebee29804e5a85d650ac50b6cf84"
IMAGE_PATH = "/Workspace/Users/pallavi.samodia@razorpay.com/Razorpay_payments-1170x508.png"
encoded_image = base64.b64encode(open(IMAGE_PATH, 'rb').read()).decode('ascii')
headers = {
    "Content-Type": "application/json",
    "api-key": GPT4V_KEY,
}

# COMMAND ----------

# Payload for the request
payload = {
  "messages": [],
  "temperature": 0.7,
  "top_p": 0.95,
  "max_tokens": 800
}

# COMMAND ----------

GPT4V_ENDPOINT = "https://everybodyhackon.openai.azure.com/openai/deployments/everybody-4o/chat/completions?api-version=2024-02-15-preview"
# Send request
try:
    response = requests.post(GPT4V_ENDPOINT, headers=headers, json=payload)
    response.raise_for_status()  # Will raise an HTTPError if the HTTP request returned an unsuccessful status code
except requests.RequestException as e:
    raise SystemExit(f"Failed to make the request. Error: {e}")
# Handle the response as needed (e.g., print or process)
print(response.json())

# COMMAND ----------

openai.api_key = AZURE_OPENAI_API_KEY
openai.api_base = AZURE_OPENAI_ENDPOINT # your endpoint should look like the following https://YOUR_RESOURCE_NAME.openai.azure.com/
openai.api_type = 'azure'
openai.api_version = '2024-02-01' # this might change in the future

deployment_name='everybody-4o' #This will correspond to the custom name you chose for your deployment when you deployed a model. 

# Send a completion call to generate an answer
print('Sending a test completion job')
start_phrase = 'Write a tagline for an ice cream shop. '
response = openai.Completion.create(engine=deployment_name, prompt=start_phrase, max_tokens=10)
text = response['choices'][0]['text'].replace('\n', '').replace(' .', '.').strip()
print(start_phrase+text)

# COMMAND ----------


