
def record_is_valid(rec, required_fields):
    # check each column is valid
    # if we don't have the column, we'll log the information.
    missing = []
    for field in required_fields:
        if field not in rec:
            missing.append(field)
    if len(missing) == 0:
        return True, missing
    else:
        return False, missing


def validate_records_simple(records, required_fields, file_name):
    #print(f'what is it insdie {records}')
    #print("typeeeeee" , type(records))
    # records will be like =:  {'_id': 101, 'url': 'http:/..'}  which is a dict
    clean = []
    errors = []
    # check the file is whether is empty

    if records is None:
        errors.append(f'{file_name}: records is None')
        return clean, errors

    # check json type whether is the list format
    if not isinstance(records, list):
        #print('hell0000000o' ,records)
        #print(type(records))
        errors.append(f"{file_name} records is not a list, got {type(records)} type")

    for i, line_rec in enumerate(records, start = 1):
        # start = 1 means i = 1 rather than 0
        if not isinstance(line_rec, dict):
            errors.append(f"{file_name}: is not dic type")
            continue
       # print(f"here is the data type:::::::{print(records)}")
        ok, missing = record_is_valid(line_rec,required_fields)
        if ok:
            clean.append(line_rec)
        else:
            errors.append(f"{file_name}: records #{i} has missing value: {missing}.")
    #print(f'hehre is {clean}')
    return clean, errors















