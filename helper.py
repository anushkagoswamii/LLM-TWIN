import os
from pymongo import MongoClient
from dotenv import load_dotenv
from litellm import completion
load_dotenv()

def llm(model_name, api_key, post, prompt):
    output = completion(
        model = "ollama/" + model_name,
        messages=[
            {"role": "system", "content": prompt},
            {"role": "user", "content": post},
        ],
        api_base = api_key
    )
    response = output.choices[0].message.content.strip()
    return response

def fetch_from_mongo(collection_name):
    client = MongoClient(os.getenv("DATABASE_HOST"))
    db = client[os.getenv("DATABASE_NAME")]
    collection = db[collection_name]
    all_content = list(collection.find())
    return all_content

def push_to_mongo(data, collection_name):
    client = MongoClient(os.getenv("DATABASE_HOST"))
    db = client[os.getenv("DATABASE_NAME")]
    collection = db[collection_name]
    collection.delete_one({"_id": data['_id']})
    result = collection.insert_one(data)
    return result.inserted_id

def get_company_url(company_names: str) -> list:
    if not company_names:
        return []
    return [f"https://www.linkedin.com/company/{company_name.strip()}/" for company_name in company_names.split(',')]

def get_company_names(company_names: str) -> list:
    if not company_names:
        return []
    return [company_name.strip() for company_name in company_names.split(',')]
