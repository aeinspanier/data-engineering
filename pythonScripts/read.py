import os
import pandas as pd


def get_json_reader(BASE_DIR, table_name, chunksize=1000):
    file = os.listdir(f'{BASE_DIR}/{table_name}')[0]
    return pd.read_json(f'{BASE_DIR}/{table_name}/{file}', lines=True, chunksize=chunksize)

if __name__ == '__main__':
    BASE_DIR = os.environ.get('BASE_DIR')
    table_name = 'departments'
    reader = get_json_reader(BASE_DIR, table_name)
    for idx, df in enumerate(reader):
        print(f'Number of records in chunk with index {idx} is {df.shape[0]}')