from datetime import datetime

def source_file_path(today):
    path = f'resource/data/raw/consumption_{today}.csv'
    return path

def desination_file_path(type, today):
    if type == 'filtered':
        filtered_path = f'./resource/data/filtered/filtered_consumption_{today}.csv'
        return filtered_path
    elif type == 'transformed':
        transformed_path = f'./resource/data/filtered/transformed_consumption_{today}.csv'
        return transformed_path
    elif type == 'alcoholic':
        alcoholic_path = f'./resource/data/filtered/consumption_alcoholic_{today}.csv'
        return alcoholic_path
    elif type == 'cereals_bakery':
        cereals_bakery_path = f'./resource/data/filtered/consumption_cereals_bakery_{today}.csv'
        return cereals_bakery_path
    elif type == 'meats_poultry':
        meats_poultry_path = f'./resource/data/filtered/consumption_meats_poultry_{today}.csv'
        return meats_poultry_path
    elif type == 'verify_data':
        verify_data_path = f'./resource/data/metrics/verify_data_{today}.txt'
        return verify_data_path
    else:
        return 'invalid type'

def db_file_path(type, today):
    if type == 'alcoholic':
        alcoholic_path = f'/resource/data/filtered/consumption_alcoholic_{today}.csv'
        return alcoholic_path
    elif type == 'cereals_bakery':
        cereals_bakery_path = f'/resource/data/filtered/consumption_cereals_bakery_{today}.csv'
        return cereals_bakery_path
    elif type == 'meats_poultry':
        meats_poultry_path = f'/resource/data/filtered/consumption_meats_poultry_{today}.csv'
        return meats_poultry_path
    else:
        return 'invalid type'