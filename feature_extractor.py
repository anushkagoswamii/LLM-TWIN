import os, re
from tqdm import tqdm
from helper import fetch_from_mongo, push_to_mongo, get_company_names, llm
from constant import GET_KEYWORDS_PROMPT, GET_TOPIC_PROMPT, GET_CLEANED_TEXT_PROMPT
from dotenv import load_dotenv
load_dotenv()


class FeatureExtractor():

    def __init__(self, MODEL_NAME = os.getenv('MODEL_NAME'), OLLAMA_URL = os.getenv('OLLAMA_URL')):
        self.MODEL_NAME = MODEL_NAME
        self.OLLAMA_URL = OLLAMA_URL

    def get_keywords(self, post):
        tries = 3
        while tries:
            response = llm(self.MODEL_NAME, self.OLLAMA_URL, post, GET_KEYWORDS_PROMPT)
            try:
                return response.split()
            except:
                tries-=1
        return None

    def get_topic(self, post):
        return llm(self.MODEL_NAME, self.OLLAMA_URL, post, GET_TOPIC_PROMPT)

    def get_clean_post(self, post):
        post = re.sub(re.compile(r'hashtag'), '', post)
        return llm(self.MODEL_NAME, self.OLLAMA_URL, post, GET_CLEANED_TEXT_PROMPT)

    def extract(self, collection_names):
        for collection_name in collection_names:
            all_data = fetch_from_mongo(collection_name)
            for data in tqdm(all_data):
                post_text = data['content']['Text']
                data['keywords'] = self.get_keywords(post_text)
                data['clean_post'] = self.get_clean_post(post_text)
                data['topic'] = self.get_topic(post_text)
                push_to_mongo(collection_name, data)



if __name__ == "__main__" :

    feature_extractor = FeatureExtractor()
    feature_extractor.extract(get_company_names(os.getenv('COMPANY_NAMES'))),
