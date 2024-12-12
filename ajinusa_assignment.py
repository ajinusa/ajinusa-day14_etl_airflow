from airflow.models.param import Param
from airflow.decorators import dag, task
import bs4
import requests
import sqlalchemy as sa
import pandas as pd
import csv
import os
from airflow.operators.empty import EmptyOperator



@dag(
    params = {
        "url": Param("https://www.antaranews.com/top-news", description="masukkan url"), # definisikan parameternya
        "filename": Param("data_berita", description="masukkan nama file"),
    }
)

def assignment():

    start_task   = EmptyOperator(task_id="start_task")
    end_task   = EmptyOperator(task_id="end_task")
    extract_from_json   = EmptyOperator(task_id="extract_from_json")

    @task
    def extract_web(param1,param2):
        # print(params)
        print(param1, type(param1))
        # print(param2, type(param2))

        response = requests.get(param1)
        soup     = bs4.BeautifulSoup(response.content, "html.parser")

        # Contoh pengambilan data: Mengambil semua teks dari tag <h2>
        data = [h2.get_text() for h2 in soup.find_all("h2")]
        # return data

    # data_news = extract_web(param1)

    # @task
    # def extract_csv(data):
        df = pd.DataFrame(data)

        # Ensure the /data directory exists
        output_dir = '/opt/airflow/data/'
        os.makedirs(output_dir, exist_ok=True)

        # Save the CSV file
        output_path = os.path.join(output_dir, param2+'.csv')
        df.to_csv(output_path, index=False)

        print(f"Data saved to {output_path}")

    @task
    def extract_from_csv(param1):
        filename = '/opt/airflow/data/'+param1+'.csv'
        # Membaca data CSV
        with open(filename, "r") as f:
            reader = csv.DictReader(f)
            data   = pd.DataFrame([row for row in reader])
        return data



    @task
    # ===================== load =====================
    def load_sqlite(df, table_name):
        engine     = sa.create_engine(f"sqlite:///data/"+table_name+".sqlite")

    # Load DataFrame ke tabel SQLite, menggantikan tabel jika sudah ada
        with engine.begin() as conn:
            df.to_sql(table_name, conn, index=False, if_exists="replace")

          # Mengambil data dengan query SQL
            query = "SELECT * FROM "+table_name+" LIMIT 10"
            df = pd.read_sql(sa.text(query), conn)

            print("Menampilkan tabel sqlite : "+table_name)
            print(df)


    # extract_web(param1 = "{{ params['url'] }}") >> extract_csv(param1 = "{{ params['url'] }}")

    start_task >> extract_web(param1 = "{{ params['url'] }}",param2 = "{{ params['filename'] }}") >> [extract_from_csv(param1 = "{{ params['filename'] }}"),extract_from_json] >> load_sqlite(df = extract_from_csv(param1 = "{{ params['filename'] }}"),table_name = "{{ params['filename'] }}") >> end_task
   


assignment()


