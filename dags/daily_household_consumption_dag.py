import os
import pandas as pd
import requests
from datetime import datetime
from urllib.parse import urljoin
from bs4 import BeautifulSoup
from airflow import DAG
from airflow.operators.python import PythonOperator

today_date = datetime.today().strftime('%d%m%Y')


def download_file():
    url = 'https://www.insee.fr/en/statistiques/6523822#tableau-conso-biens-g1-en'
    response = requests.get(url)

    if response.status_code == 200:
        soup = BeautifulSoup(response.content, 'html.parser')
        file_link = soup.find('div', {'class': 'donnees-telechargeables'})

        if file_link:
            file_url = urljoin(url, file_link.find('a').get('href'))
            file_response = requests.get(file_url)

            if file_response.status_code == 200:
                save_path = f'./data/raw/data_{today_date}.xlsx'
                with open(save_path, 'wb') as file:
                    file.write(file_response.content)
                print(f"File downloaded successfully at: {save_path}")
                return save_path
            else:
                print("Failed to download the file.")
        else:
            print("Download link not found on the page.")
    else:
        print("Failed to fetch the webpage.")
    return None


def clean_data():
    filename = f'./data/raw/data_{today_date}.xlsx'

    def fetch_data(filename, sheet_name):
        data = pd.read_excel(filename, sheet_name=sheet_name, index_col=None, header=None, engine='openpyxl')
        data.drop(index=data.index[:11], axis=0, inplace=True)
        data.drop(columns=16, inplace=True)

        expected_columns = ['date', 'total', 'food_product', 'food_product_except_tobacco', 'eng_prod',
                            'eng_prod_durab',
                            'eng_prod_durab_transport_equipment', 'eng_prod_durab_household_durables',
                            'eng_prod_durab_other_durables', 'eng_prod_textile_lether',
                            'eng_prod_other_engineered_goods',
                            'energy', 'energy_water_waste', 'energy_fuel_and_oil',
                            'energy_including_petrolieam_product',
                            'energy_expept_petroleam_products', 'manufactured_goods']

        if len(data.columns) == len(expected_columns):
            data.columns = expected_columns
        else:
            print(f"Column mismatch: Expected {len(expected_columns)} columns, got {len(data.columns)}")
            return None

        return data

    contributions_data = fetch_data(filename, "Contributions")
    evolution_changes_data = fetch_data(filename, "Evolutions - Changes")
    niveaux_levels = fetch_data(filename, "Niveaux - Levels")

    if contributions_data is not None and evolution_changes_data is not None and niveaux_levels is not None:
        folder = f'data/prepared/{today_date}'
        os.makedirs(folder, exist_ok=True)

        contributions_data.to_csv(f"{folder}/contributions_data.csv")
        evolution_changes_data.to_csv(f"{folder}/evolution_changes_data.csv")
        niveaux_levels.to_csv(f"{folder}/niveaux_levels.csv")
    else:
        print("Failed to clean and save the data.")


dag = DAG(
    'download_and_prepare_data',
    description='DAG for downloading and cleaning data',
    schedule_interval=None,
    start_date=datetime(2021, 4, 1),
    tags=["household", "consumption", "data-download"],
    catchup=False
)

dag.doc_md = """
DAG for download and prepared data.
TODO: ADD MANUAL TRIGGER AND MORE DOCUMENTATION ABOUT THE DAG.
"""

download_daily_file = PythonOperator(
    task_id='download_file',
    python_callable=download_file,
    dag=dag
)

clean_data_task = PythonOperator(
    task_id='clean_data',
    python_callable=clean_data,

    dag=dag
)

download_daily_file >> clean_data_task
