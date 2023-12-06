import requests
import json


class Data_extractor:
    def __init__(self):
        print('---------- Initializing the extraction instance ----------')

    def get_json_data(self, url: str, num_days: int) -> list:
        complete_url = f'{url}/{num_days}' 
        r = requests.get(complete_url)
        json_data = json.loads(r.text)
        return json_data
    

if __name__ == '__main__':
    extract_obj = Data_extractor()
    data = extract_obj.get_json_data(
        url = 'https://economia.awesomeapi.com.br/json/daily/BTC-BRL',
        num_days = 10
    )

    for row in data:
        print(row)

    
    