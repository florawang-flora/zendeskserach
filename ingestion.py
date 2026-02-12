import json
import os
import os
class Ingestion:
    def __init__(self, base_path):
        self.base_path = base_path
        self.data_source = [ 'organizations.json', 'tickets.json', 'users.json']
    def get_load_all_json(self):
        return self.__load_all_json()


    def __load_all_json(self):
        # file system check
        all_data = {}
        for file_name in self.data_source:
            file_path = os.path.join(self.base_path, file_name)
            print(f'Trying to load data file {file_path}')
            if not os.path.exists(file_path):
                print(f"We didn't load the file: {file_name}")
                all_data[file_name] = None
                continue
        # load the json file
        try:
            with open(file_path) as f:
                all_data[file_name] = json.load(f)
                print(f'Successfully load the file {file_name}')
        except json.JSONDecodeError:
            print(f"JSON format error : {file_name}")
            all_data[file_name] = None

        print("Loaded keys:", all_data.keys())
        return all_data



if __name__ == "__main__":
    base_path = '/Users/mac/PycharmProjects/Zendesk_research/data_source/'
    ingest = Ingestion(base_path)
    load_json_data = ingest.get_load_all_json()
    print(load_json_data)









if __name__ == '__main__':
    base_path ="/Users/mac/PycharmProjects/Zendesk_research/data_source/"
    ingestion_data = Ingestion(base_path)
    data = ingestion_data.__load_all_json()
    print(data)

#    data = load_json()
#    print(data)
#    print(data.keys())
#    tickets = data['tickets.json']
#    organization = data['organizations.json']
#    users = data['users.json']
#    #print(tickets[:1])
#    #print(organization[:1])
#    #print(users[:1])







