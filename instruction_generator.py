from pymongo import MongoClient, errors
from litellm import completion
import json
from tqdm import tqdm
import config


class InstructionGenerator():

    def __init__(self, USER_PROMPT, SYSTEM_PROMPT):
        self.USER_PROMPT = USER_PROMPT
        self.SYSTEM_PROMPT = SYSTEM_PROMPT

    def fetch_all_content(self, collection_name):
        client = MongoClient(config.DATABASE_HOST)
        db = client[config.DATABASE_NAME]
        collection = db[collection_name]
        all_content = [[data['_id'],data['content']['Text']] for data in list(collection.find())]
        return all_content

    def format_data(self, data_points, is_example, start_index):
        text = ""
        for index, data_point in enumerate(data_points):
            if not is_example:
                text += f"Content number {start_index + index }\n"
            text += str(data_point) + "\n"
        return text

    def format_batch(self, context_msg, data_points, start_index):
        delimiter_msg = context_msg
        delimiter_msg += self.format_data(data_points, False, start_index)
        return delimiter_msg

    def format_prompt(self, inference_posts, start_index):
        initial_prompt = self.USER_PROMPT
        initial_prompt += f"You must generate exactly a list of {len(inference_posts)} json objects, using the contents provided under CONTENTS FOR GENERATION\n"
        initial_prompt += self.format_batch(
            "\nCONTENTS FOR GENERATION: \n", inference_posts, start_index
        )
        return initial_prompt

    def clean_response(self, response):
        response = str(response)
        start_index = response.rfind("[")
        end_index = response.rfind("]")
        return response[start_index : end_index + 1]

    def send_prompt(self, prompt):
        while True:
            try:
                output = completion(
                    model="ollama/llama3",
                    messages=[
                        {"role": "system", "content": self.SYSTEM_PROMPT},
                        {"role": "user", "content": prompt[:16384]},
                    ], 
                    api_base = "http://localhost:11434"
                )
                response = output.choices[0].message.content
                return json.loads(self.clean_response(response))
            except:
                pass

    def push_to_mongo(self, collection_name, data):
        client = MongoClient(config.DATABASE_HOST)
        db = client[config.DATABASE_NAME]
        try:
            collection = db[collection_name+'_instructions']
            collection.delete_one({"_id": data['_id']})
            result = collection.insert_one(data)
            return result.inserted_id
        except errors.WriteError as e:
            print(f'Failed to insert data into mongodb {e} \nFor {collection_name}')
            return None

    def generate_instructions(self, collection_names, batch_size=1):
        for collection_name in collection_names:
            all_contents = self.fetch_all_content(collection_name)
            inserted_ids = []
            for i in tqdm(range(0, len(all_contents), batch_size)):
                batch = [content[1] for content in all_contents[i : i + batch_size]]
                initial_prompt = self.format_prompt(batch, i)
                data = self.send_prompt(initial_prompt)
                for j in range(i, i + batch_size):
                    data[j-i]["content"] = all_contents[j][1]
                    data[j-i]["_id"] = all_contents[j][0]
                    inserted_ids.append(all_contents[j][0])
                    self.push_to_mongo(collection_name, data[j-i])

        return inserted_ids



if __name__ == "__main__" :

    USER_PROMPT = (
        f"I will give you batches of contents of posts. Please generate me exactly 1 instruction for each of them. "
        f"The instructions should be a relevant linkedin post title and generic."
        f"The posts text for which you have to generate the instructions is under Content number x lines. Please structure the answer in json format,"
        f"ready to be loaded by json.loads(), a list of objects only with fields called instruction and content. For the content field, copy the number of the content only!."
        f"Please do not add any extra characters and make sure it is a list with objects in valid json format!\n"
    )
    SYSTEM_PROMPT = "You are a technical automated linkedin post writer handling someone's account to generate posts."


    instruction_generator = InstructionGenerator(USER_PROMPT, SYSTEM_PROMPT)
    instruction_generator.generate_instructions(config.COMPANY_NAMES),

