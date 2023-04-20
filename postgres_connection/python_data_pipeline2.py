import os
import datetime as dt
from datetime import timedelta

import pandas as pd

from faker import Faker
import csv


def generate_csv(save_path):
    print("Generating CSV file...")
    os.makedirs(save_path, exist_ok=True)
    output=open(os.path.join(save_path, 'data.csv'), 'w')
    fake=Faker()
    header=['name','age','street','city','state','zip','lng','lat']
    mywriter=csv.writer(output)
    mywriter.writerow(header)
    for r in range(1000):
        mywriter.writerow([fake.name(),fake.random_int(min=18, max=80, step=1), fake.street_address(), fake.city(),fake.state(),fake.zipcode(),fake.longitude(),fake.latitude()])
    output.close()


def csv_to_json(read_path, save_path):
    print("Converting CSV to JSON...")
    os.makedirs(read_path, exist_ok=True)
    os.makedirs(save_path, exist_ok=True)
    df = pd.read_csv(os.path.join(read_path, "data.csv"))
    for i, r in df.iterrows():
        print(r['name'])
    df.to_json(os.path.join(save_path, "fromAirflow.json"), orient='records')