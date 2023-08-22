import pandas as pd
import requests
import json

# Defina a URL da API
api_url = 'https://sdw-2023-prd.up.railway.app/users'

# pega a lista de ids no SDW2023.csv #

Ids = pd.read_csv('SDW2023.csv')
user_ids = Ids['UserID'].tolist()
print(user_ids)


def get_user(id):
    response = requests.get(f'{api_url}/{id}')
    return response.json() if response.status_code == 200 else None


users = [user for id in user_ids if (user := get_user(id)) is not None]
print(json.dumps(users, indent=2))

# Função para extrair dados da API


def extract_data_from_api(api_url):
    response = requests.get(api_url)
    if response.status_code == 200:
        data = response.json()
        return data
    else:
        raise Exception("Failed to fetch data from API." +
                        "Status code: {response.status_code}")

# Função para transformar os dados


def transform_data(data):
    # Exemplo de transformação: criar um DataFrame a partir dos dados
    df = pd.DataFrame(data)

    # Outras transformações...

    return df

# Função para carregar dados em um arquivo CSV


def load_data_to_csv(data, destination_path):
    data.to_csv(destination_path, index=False)

# Função que representa o Pipeline de ETL


def etl_pipeline(api_url, destination_path):
    data = extract_data_from_api(api_url)
    transformed_data = transform_data(data)
    load_data_to_csv(transformed_data, destination_path)
    print("ETL Pipeline completed successfully.")

# Execução do Pipeline


if __name__ == "__main__":
    destination_path = 'data.csv'
    etl_pipeline(api_url, destination_path)
