import requests
import json


class Data_extractor:
    def __init__(self, project_name):
        self.project_name = project_name

    def get_json_data(self, url: str, num_days: int) -> list:
        """
        This method gets data from a specific API URL and transforms it into a list.
        """
        complete_url = f'{url}/{num_days}' 
        r = requests.get(complete_url)
        json_data = json.loads(r.text)
        return json_data
    

if __name__ == '__main__':
    extract_obj = Data_extractor(project_name = 'Currency daily quotation')

    print(extract_obj.project_name)

    data = extract_obj.get_json_data(
        url = 'https://economia.awesomeapi.com.br/json/daily/BTC-BRL',
        num_days = 100
    )

    for row in data:
        print(row)
        