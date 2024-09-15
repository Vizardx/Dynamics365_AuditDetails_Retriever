import pandas as pd
import requests
import adal
import json
import config
from datetime import datetime
import time
import concurrent.futures
import threading
import csv

context = adal.AuthenticationContext(config.authority_url)
token = context.acquire_token_with_client_credentials(config.resource_url, config.client_id, config.client_secret)

headers = {
    'Authorization': 'Bearer ' + token['accessToken'],
    'OData-MaxVersion': '4.0',
    'OData-Version': '4.0',
    'Accept': 'application/json',
    'Content-Type': 'application/json; charset=utf-8',
    'Prefer': 'odata.maxpagesize=500',
    'Prefer': 'odata.include-annotations=OData.Community.Display.V1.FormattedValue'
}

# Lee el archivo CSV de proyecto KWS Integration Project 1/CRM to CSV/Audit
audit_df = pd.read_csv('audit.csv')

# Convierte la columna 'createdon' a datetime
audit_df['createdon'] = pd.to_datetime(audit_df['createdon'])

# Filtra las filas donde objecttypecode es 'account' o 'contact', operationname es 'Update' o 'Create', y createdon es posterior al 1 de agosto de 2023
filtered_df = audit_df[(audit_df['objecttypecode'].isin(['account'])) & (audit_df['operationname'].isin(['Update'])) & (audit_df['createdon'] > datetime(2023, 12, 1))]

# Crea una lista vacía para almacenar todos los datos
all_data = []
contador = 0

# Guarda la hora en que se adquirió el token
token_acquired_time = time.time()

# Crea un semáforo para limitar el número de tareas concurrentes
semaphore = threading.Semaphore(2500)  # Ajusta este número según tus necesidades

def process_audit_id(audit_id):
    global contador
    # Elimina los corchetes del ID de auditoría
    audit_id = audit_id.strip('{}')

    with semaphore:
        while True:  # Este es el bucle que se repite hasta que la solicitud sea exitosa
            try:
                # Realiza la solicitud
                response = requests.get(config.resource_url + '/api/data/v9.0/audits(' + audit_id + ')/Microsoft.Dynamics.CRM.RetrieveAuditDetails', headers=headers)
                print(f"Iterando audit id : {audit_id} . . .")

                if response.status_code == 429:
                    # Si recibimos un error 429, esperamos un segundo y luego volvemos a intentarlo
                    print("Recibido error 429, esperando un segundo...")
                    time.sleep(1)
                    continue  # Esto vuelve al inicio del bucle while, no al bucle exterior

                # Parsea la respuesta JSON
                data = json.loads(response.text)

                # Agrega el ID de auditoría a los datos
                data['audit_id'] = audit_id

                # Agrega los datos a la lista
                all_data.append(data)
                contador+=1
                print(f"OK  -  {contador} Filas iteradas correctamente")

                break  # Esto sale del bucle while una vez que la solicitud es exitosa

            except json.JSONDecodeError:
                print(f"Error al parsear la respuesta JSON para el ID de auditoría {audit_id}. Guardando la respuesta en un archivo para su revisión.")
                with open(f'error.json', 'a') as f:
                    f.write(response.text)

    # Si han pasado 50 minutos desde que se adquirió el token, renuévalo
    if time.time() - token_acquired_time >= 50 * 60:  # 50 minutos
        print("Han pasado 50 minutos, obteniendo un nuevo token...")
        token = context.acquire_token_with_client_credentials(config.resource_url, config.client_id, config.client_secret)
        headers['Authorization'] = 'Bearer ' + token['accessToken']
        token_acquired_time = time.time()

# Crea un ThreadPoolExecutor
with concurrent.futures.ThreadPoolExecutor() as executor:
    # Itera sobre los ID de auditoría en el DataFrame filtrado en lotes de 1000
    for i in range(0, len(filtered_df['auditid']), 3000):
        # Lanza una tarea para procesar este lote de ID de auditoría
        executor.map(process_audit_id, filtered_df['auditid'][i:i+1000])

# Escribe los datos en un archivo JSON
with open('output3.json', 'w') as f:
    json.dump(all_data, f, indent=4)

# Creamos un set para almacenar las claves
keys = set()

# Recorremos el json para obtener todas las claves posibles
for record in all_data:
    keys.update("OldValue_" + key for key in record["AuditDetail"]["OldValue"].keys())
    keys.update("NewValue_" + key for key in record["AuditDetail"]["NewValue"].keys())

rows = []

# Recorremos de nuevo el json para llenar la lista de filas
for record in all_data:
    row = {}
    for key in record["AuditDetail"]["OldValue"].keys():
        row["OldValue_" + key] = record["AuditDetail"]["OldValue"][key]
    for key in record["AuditDetail"]["NewValue"].keys():
        row["NewValue_" + key] = record["AuditDetail"]["NewValue"][key]
    row["audit_id"] = record["audit_id"]
    rows.append(row)

df = pd.DataFrame(rows, columns=list(keys) + ["audit_id"])

df.to_csv("AuditDetails_10-1-24.csv", index= False, quoting=csv.QUOTE_ALL)
