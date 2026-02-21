import json
import os
#print(os.getcwd())

import json

def load_json():
    all_data = {}
    base_path = "/Users/mac/PycharmProjects/Zendesk_research/data_source/"
    data_source =[ 'organizations.json', 'tickets.json', 'users.json']
    # read the data
    for file_name in data_source:
        file_path = os.path.join(base_path, file_name)
        print(f'Trying to load data from {file_path}')
        if not os.path.exists(file_path):
            print(f"We didn't load the file : {file_name}")
            all_data[file_name] = None
            # this will make sure our pipeline won't stop
            continue
        try:
            # read the data source
            with open (file_path) as f:
                all_data[file_name] = json.load(f)
                print(f"Successfully load the file: {file_name}")
            # check the JSON format if invalid format.

        except json.JSONDecodeError:
            print(f'JSON format error: {file_name}')
            all_data[file_name] = None
    return all_data

#if __name__ == '__main__':
#    data = load_json()
#    print(data)
#    print(data.keys())
#    tickets = data['tickets.json']
#    organization = data['organizations.json']
#    users = data['users.json']
#    #print(tickets[:1])
#    #print(organization[:1])
#    #print(users[:1])







