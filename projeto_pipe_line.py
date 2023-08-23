import pandas as pd
csv_path = './celeb_data.csv'

# Função para extrair os dados do CSV


def extract_data_from_csv(csv_path):
    df = pd.read_csv(csv_path)
    return df

# Função para transformar os dados


def transform_data(df):
    # Realize as transformações desejadas

    df['Awards'] = df['Awards'].str.extract(r'(\d+)').astype(float)
    # Extrai os números de prêmios como float

    df['Famous_for'] = df['Famous_for'].str.strip('"')
    # Remove as aspas do campo Famous_for
    print(df)
    return df

# Função para carregar os dados transformados


def load_transformed_data(df, destination_path):
    df.to_csv(destination_path, index=False)
    print("Dados transformados carregados com sucesso.")

# Função que representa o Pipeline de ETL


def etl_pipeline(csv_path, destination_path):
    extracted_data = extract_data_from_csv(csv_path)
    transformed_data = transform_data(extracted_data)
    load_transformed_data(transformed_data, destination_path)

# Execução do Pipeline


if __name__ == "__main__":
    destination_path = 'dados_transformados.csv'
    etl_pipeline(csv_path, destination_path)
