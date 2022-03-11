import os
import requests
import json
import smtplib
import sys
import csv
import pyodbc
import pysftp

from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

def getSecret(vault_name, secret_name):
    #Get acess token to azure account
    data = { "grant_type" : "client_credentials", 
            "client_id" : os.getenv('AZURE_CLIENT_ID').replace('"', ''), 
            "client_secret" : os.getenv('AZURE_CLIENT_SECRET').replace('"', ''), 
            "resource" : "https://vault.azure.net"
        }
    headers = { "Content-Type" : "application/x-www-form-urlencoded" }
    r = requests.post("https://login.windows.net/{}/oauth2/token".format(os.getenv('AZURE_TENANT_ID').replace('"', '')), data=data, headers=headers)
    access_token = json.loads(r.text)['access_token']
    #Get secret from KeyVault
    headers = {"Authorization":"Bearer {}".format(access_token) }
    r = requests.get('https://{}.vault.azure.net/secrets/{}?api-version=2016-10-01'.format(vault_name.replace('"', ''), secret_name), headers=headers)
    result = json.loads(r.text)
    if 'value' in result.keys():
        return result["value"]
    else: 
        return 'Secret Not Found'

pier = {
    "base_url": getSecret(os.getenv("KEY_VAULT_NAME"), "API-PIER-URL"),
    "headers": {
        "access_token": getSecret(os.getenv("KEY_VAULT_NAME"), "API-PIER-TOKEN"),
        "client_id": getSecret(os.getenv("KEY_VAULT_NAME"), "API-PIER-CLIENTID"),
        "Content-Type": "application/json"
    }
}

def getDatabaseCursos():
    host = getSecret(os.getenv("KEY_VAULT_NAME"), "DB-DW-HOST")
    database = getSecret(os.getenv("KEY_VAULT_NAME"), "DB-DW-DATABASE")
    username = getSecret(os.getenv("KEY_VAULT_NAME"), "DB-DW-RDKAUMENTOLTAUTOMATICO-USERNAME")
    password = getSecret(os.getenv("KEY_VAULT_NAME"), "DB-DW-RDKAUMENTOLTAUTOMATICO-PASSWORD")
    db_conf = {
       'server': f'{host}',
       'database': f'{database}',
       'username': f'{username}',
       'password': f'{password}',
       'driver': '{ODBC Driver 17 for SQL Server}'
    }
    conn = pyodbc.connect('DRIVER={0};SERVER={1};PORT=1433;DATABASE={2};UID={3};PWD={4}'.format(db_conf['driver'], db_conf['server'], db_conf['database'], db_conf['username'], db_conf['password']))
    return conn.cursor()

def ajusteLimite(id_conta, limite_global, limite_parcelado):
    try:
        url_ajuste_limite = pier["base_url"] + f"limites-disponibilidades?limiteGlobal={limite_global}&idConta={id_conta}"
        response_ajuste_limite = requests.put(url=url_ajuste_limite,
                                                headers=pier["headers"])
        if response_ajuste_limite.status_code == 404:
            return None
        elif response_ajuste_limite.status_code == 200:
            return json.loads(response_ajuste_limite.text)
        else:
            print(response_ajuste_limite.text)
            return None
    except Exception as ex:
        print(ex)
        return None

def getContasPrimeiroAumento():
    try:
        cursor = getDatabaseCursos()
        query = f""" 
            SELECT L.ID_CONTA
                 , L.CALC_AUMENTO_LIM1
                 , L.NU_CPF
                 , C.ID_PRODUTO
            FROM T_AUMENTO_LIMITE L, T_CONTA C
            WHERE C.ID_CONTA = L.ID_CONTA
                AND L.FLAG_AUMENTO_LIMITE = 'S'
                AND L.FL_APT_FAT_1 = 1
                AND L.FL_RECEBEU_LIT1 = 0
                AND L.CALC_AUMENTO_LIM1 IS NOT NULL
                AND C.ID_STATUS_CONTA IN ('0','10')
                AND L.DT_CADASTRAMENTO_CONTA BETWEEN (C.DT_ATIVACAO - 180) AND C.DT_ATIVACAO
                AND L.CALC_AUMENTO_LIM1 > L.VL_LIMITE_GLOBAL
        """
        cursor.execute(query) 
        row = cursor.fetchall()
        return row
    except Exception as ex:
        print(ex)


def getContasSegundoAumento():
    try:
        cursor = getDatabaseCursos()
        query = f""" 
            SELECT L.ID_CONTA
                 , L.CALC_AUMENTO_LIM2
                 , L.NU_CPF
                 , C.ID_PRODUTO
            FROM T_AUMENTO_LIMITE L, T_CONTA C
            WHERE C.ID_CONTA = L.ID_CONTA
                AND L.FLAG_AUMENTO_LIMITE = 'S'
                AND L.FL_APT_FAT_1 = 1
                AND L.FL_RECEBEU_LIT1 = 1
                AND L.FL_APT_FAT_2 = 1
                AND L.FL_RECEBEU_LIT2 = 0
                AND L.CALC_AUMENTO_LIM2 IS NOT NULL
                AND C.ID_STATUS_CONTA IN ('0','10')
                AND L.DT_CADASTRAMENTO_CONTA BETWEEN (C.DT_ATIVACAO - 180) AND C.DT_ATIVACAO
                AND L.CALC_AUMENTO_LIM2 > L.VL_LIMITE_GLOBAL
                AND L.CALC_AUMENTO_LIM2 > L.CALC_AUMENTO_LIM1
        """
        cursor.execute(query) 
        row = cursor.fetchall()
        return row
    except Exception as ex:
        print(ex)

def importFtp(file_name, data):
    cnopts = pysftp.CnOpts()
    cnopts.hostkeys = None   
    with pysftp.Connection(host=getSecret(os.getenv("KEY_VAULT_NAME"), "FTP-SALESFORCE-HOST"),
        username=getSecret(os.getenv("KEY_VAULT_NAME"), "FTP-SALESFORCE-USER"),
        password=getSecret(os.getenv("KEY_VAULT_NAME"), "FTP-SALESFORCE-PASS"), cnopts=cnopts) as sftp:
        try:
            sftp.remove(f'/Import/{file_name}')
        except Exception as e:
            print(e)
        sftp.put(f'{file_name}', f'/Import/{file_name}')
        with sftp.open(f'/Import/{file_name}', 'w') as f:
            f.write((f"{data}"))
            f.close()

def getLimiteParcelado(id_produto, limite_parcelado):
    if id_produto == 455:
        limite_parcelado = limite_parcelado * 2
    elif id_produto == 453:
        limite_parcelado = limite_parcelado * 2
    elif id_produto == 415:
        limite_parcelado = limite_parcelado * 7 
    elif id_produto == 505:
        limite_parcelado = limite_parcelado * 5
    elif id_produto == 354 or id_produto == 451:
        limite_parcelado = limite_parcelado * 2
    elif id_produto == 484:
        limite_parcelado = limite_parcelado * 2
    elif id_produto == 471:
        limite_parcelado = limite_parcelado * 2
    elif id_produto == 519:
        limite_parcelado = limite_parcelado * 2
    elif id_produto == 513:
        limite_parcelado = limite_parcelado * 5

    return limite_parcelado

now = datetime.now()  # current date and time
date_time = now.strftime("%d/%m/%Y %H:%M:%S")
print("RUN CRON JOB")
print(date_time)

date = now.strftime("%Y-%m-%d")
contas_primeiro_aumento_csv = "ID_CONTA,CALC_AUMENTO_LIM1,DATA_AUMENTO,NU_CPF\n"

file_name_primeiro_aumento = "receberam_1limite_automatico.csv"
file_primeiro_aumento = open(f'{file_name_primeiro_aumento}',"w+")
contas_primeiro_aumento = getContasPrimeiroAumento()
total_contas_primeiro_aumento = 0

for conta in contas_primeiro_aumento:
    limite_global = conta[1]
    limite_parcelado = conta[1]

    limite_parcelado = getLimiteParcelado(int(conta[3]), limite_parcelado)
    response = ajusteLimite(conta[0], limite_global, limite_parcelado)
    if response != None:
        print(f'PRIMEIRO AUMENTO REALIZADO ID_CONTA: {conta[0]}, PRODUTO: {int(conta[3])}, LIMITE GLOBAL: {limite_global}, LIMITE PARCELADO: {limite_parcelado}')
        data_aumento = now.strftime("%Y-%m-%d")
        contas_primeiro_aumento_csv += f'{conta[0]},{conta[1]},{data_aumento},{conta[2]}\n'
        total_contas_primeiro_aumento += 1

importFtp(file_name_primeiro_aumento, contas_primeiro_aumento_csv)
file_primeiro_aumento.close()
os.remove(file_name_primeiro_aumento)

contas_segundo_aumento_csv = "ID_CONTA,CALC_AUMENTO_LIM2,DATA_AUMENTO,NU_CPF\n"

file_name_segundo_aumento = "receberam_2limite_automatico.csv"
file_segundo_aumento = open(f'{file_name_segundo_aumento}',"w+")
contas_segundo_aumento = getContasSegundoAumento()
total_contas_segundo_aumento = 0

for conta in contas_segundo_aumento:
    limite_global = conta[1]
    limite_parcelado = conta[1]

    limite_parcelado = getLimiteParcelado(int(conta[3]), limite_parcelado)
    response = ajusteLimite(conta[0], limite_global, limite_parcelado)
    if response != None:
        print(f'SEGUNDO AUMENTO REALIZADO ID_CONTA: {conta[0]}, PRODUTO: {int(conta[3])}, LIMITE GLOBAL: {limite_global}, LIMITE PARCELADO: {limite_parcelado}')
        data_aumento = now.strftime("%Y-%m-%d")
        contas_segundo_aumento_csv += f'{conta[0]},{conta[1]},{data_aumento},{conta[2]}\n'
        total_contas_segundo_aumento += 1

importFtp(file_name_segundo_aumento, contas_segundo_aumento_csv)
file_segundo_aumento.close()
os.remove(file_name_segundo_aumento)
