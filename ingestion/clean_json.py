# this is to clean the json file strucutre.
## step1: if we don't have the key column in the raw data source, we're going to add the key: value(None)
## step2: generalize a standarlize string format.
# return clean json format
# for the next file -> change to the dataframe.

def clean_json(records, required_fields):
    clean_records=[]
    """
    :param records:list[dict]
    :param required_fields: str
    :return: list[dict]
    """
    #print('here is the recordddss', records)
    for dict1 in records:
        clean_dict = {}
        for key1 in required_fields:
            # like a coalesce function
            # step1
            # if we don't have the field, create it as None.
            value = dict1.get(key1, None)

            # step2
            # standardlize the string
            if isinstance(value, str):
                value = value.strip()

            clean_dict[key1] = value
        clean_records.append(clean_dict)

    return clean_records











