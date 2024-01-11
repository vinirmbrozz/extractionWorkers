import json, sys, time

import httpx
import redis
from datetime import datetime

from Logger import LogService
from Exchange import QueueExchange

versao = "v1.01 - 17/11/2023 Logger Exchange e Redis "
versao = "v1.02 - 28/11/2023 Ajustes no padrão de execução "
versao = "v1.03 - 30/11/2023 Envio de messagem entre processos com 'serviço:msg'"
versao = "v1.04 - 05/12/2023 Controle da cargas de dados: dashboard.atualizacoes"
versao = "v1.05 - M.J. 15/12/23 - Adaptação novo Logger e Exchange"
versao = "v1.06 - M.J. 17/12/23 - Sem atualizações disponíveis -> Status= Atualizado"

def collector(env):
    # ARMAZENAMENTO DOS DADOS SIENGE SALES CONTRACT EMITIDOS
    logger.logMsg("ARMAZENAMENTO DOS DADOS SIENGE SALES CONTRACT EMITIDOS")
    offset = 0
    limit = 200
    statusOK = True
    vendas = []
    data_inicial = f"{datetime.now().year - 1}-01-01"
    print(data_inicial)

    with httpx.Client(auth=(env["usrSienge"], env["pwdSienge"])) as client:
        while statusOK:
            url = f"https://api.sienge.com.br/{env['cltSienge']}/public/api/v1/sales-contracts?situation=2&limit={limit}&offset={offset}"
            response = client.get(url)
            if response.status_code != 200:
                logger.logMsg(f"Erro: {response.status_code}")
                if response.status_code == 429: # Limite de requisições excedido
                    logger.logMsgError("Limite de requisições excedido")
                    time.sleep(10)
                    continue
                statusOK = False
                break
            for items in response.json()["results"]:
                if items["contractDate"] >= data_inicial:
                    vendas.append(items)
            qtdDados = len(response.json()["results"])
            offset = offset + qtdDados
            if qtdDados == 0:
                statusOK = False

    json_vendas = json.dumps(vendas)
    rd.set("loader_sales_contracts_emitido", json_vendas)

    # ARMAZENAMENTO DOS DADOS SIENGE SALES CONTRACT CANCELADOS
    logger.logMsg("ARMAZENAMENTO DOS DADOS SIENGE SALES CONTRACT CANCELADOS")
    offset = 0
    limit = 200
    statusOK = True
    vendas = []
    with httpx.Client(auth=(env["usrSienge"], env["pwdSienge"])) as client:
        while statusOK:
            url = f"https://api.sienge.com.br/{env['cltSienge']}/public/api/v1/sales-contracts?situation=3&limit={limit}&offset={offset}"
            response = client.get(url)
            if response.status_code != 200:
                logger.logMsgError(f"Erro: {response.status_code}")
                if response.status_code == 429: # Limite de requisições excedido
                    logger.logMsgError("Limite de requisições excedido")
                    time.sleep(10)
                    continue
                statusOK = False
                break
            for items in response.json()["results"]:
                if items["contractDate"] >= data_inicial:
                    vendas.append(items)
            qtdDados = len(response.json()["results"])
            offset = offset + qtdDados
            if qtdDados == 0:
                statusOK = False

    json_vendas = json.dumps(vendas)
    rd.set("loader_sales_contracts_cancelado", json_vendas)

    # ARMAZENAMENTO DOS DADOS SIENGE UNITS
    logger.logMsg("ARMAZENAMENTO DOS DADOS SIENGE UNITS")
    offset = 0
    limit = 200
    statusOK = True
    vendas = []
    with httpx.Client(auth=(env["usrSienge"], env["pwdSienge"])) as client:
        while statusOK:
            url = f"https://api.sienge.com.br/{env['cltSienge']}/public/api/v1/units?limit={limit}&offset={offset}"
            response = client.get(url)
            if response.status_code != 200:
                logger.logMsgError(f"Erro: {response.status_code} ")
                if response.status_code == 429: # Limite de requisições excedido
                    logger.logMsgError("Limite de requisições excedido")
                    time.sleep(10)
                    continue
                statusOK = False
                break
            for items in response.json()["results"]:
                vendas.append(items)
            qtdDados = len(response.json()["results"])
            offset = offset + qtdDados
            if qtdDados == 0:
                statusOK = False

    json_vendas = json.dumps(vendas)
    rd.set("loader_vendas_units_estoque", json_vendas)

    # ARMAZENAMENTO DOS DADOS SIENGE COMPANIES
    logger.logMsg("ARMAZENAMENTO DOS DADOS SIENGE COMPANIES")
    offset = 0
    limit = 200
    statusOK = True
    vendas = []
    with httpx.Client(auth=(env["usrSienge"], env["pwdSienge"])) as client:
        while statusOK:
            url = f"https://api.sienge.com.br/{env['cltSienge']}/public/api/v1/companies?limit={limit}&offset={offset}"
            response = client.get(url)
            if response.status_code != 200:
                logger.logMsgError(f"Erro: {response.status_code} ")
                if response.status_code == 429: # Limite de requisições excedido
                    logger.logMsgError("Limite de requisições excedido")
                    time.sleep(10)
                    continue
                statusOK = False
                break
            for items in response.json()["results"]:
                vendas.append(items)
            qtdDados = len(response.json()["results"])
            offset = offset + qtdDados
            if qtdDados == 0:
                statusOK = False

    json_vendas = json.dumps(vendas)
    rd.set("loader_vendas_companies", json_vendas)

    # ARMAZENAMENTO DOS DADOS SIENGE ENTERPRISES
    logger.logMsg("ARMAZENAMENTO DOS DADOS SIENGE ENTERPRISES")
    offset = 0
    limit = 200
    statusOK = True
    vendas = []
    with httpx.Client(auth=(env["usrSienge"], env["pwdSienge"])) as client:
        while statusOK:
            url = f"https://api.sienge.com.br/{env['cltSienge']}/public/api/v1/enterprises?limit={limit}&offset={offset}"
            response = client.get(url)
            if response.status_code != 200:
                logger.logMsgError(f"Erro: {response.status_code} ")
                if response.status_code == 429: # Limite de requisições excedido
                    logger.logMsgError("Limite de requisições excedido")
                    time.sleep(10)
                    continue
                statusOK = False
                break
            for items in response.json()["results"]:
                vendas.append(items)
            qtdDados = len(response.json()["results"])
            offset = offset + qtdDados
            if qtdDados == 0:
                statusOK = False

    json_vendas = json.dumps(vendas)
    rd.set("loader_vendas_enterprises", json_vendas)

    # ARMAZENAMENTO DOS DADOS SIENGE CUSTOMERS
    logger.logMsg("ARMAZENAMENTO DOS DADOS SIENGE CUSTOMERS")
    offset = 0
    limit = 200
    statusOK = True
    vendas = []
    with httpx.Client(auth=(env["usrSienge"], env["pwdSienge"])) as client:
        while statusOK:
            url = f"https://api.sienge.com.br/{env['cltSienge']}/public/api/v1/customers?limit={limit}&offset={offset}"
            response = client.get(url)
            if response.status_code != 200:
                logger.logMsgError(f"Erro: {response.status_code} ")
                if response.status_code == 429: # Limite de requisições excedido
                    logger.logMsgError("Limite de requisições excedido")
                    time.sleep(10)
                    continue
                statusOK = False
                break
            for items in response.json()["results"]:
                vendas.append(items)
            qtdDados = len(response.json()["results"])
            offset = offset + qtdDados
            if qtdDados == 0:
                statusOK = False

    json_vendas = json.dumps(vendas)
    rd.set("loader_customers", json_vendas)

    # ARMAZENAMENTO DOS DADOS SIENGE CREDITORS
    logger.logMsg("ARMAZENAMENTO DOS DADOS SIENGE CREDITORS")
    offset = 0
    limit = 200
    statusOK = True
    vendas = []
    with httpx.Client(auth=(env["usrSienge"], env["pwdSienge"])) as client:
        while statusOK:
            url = f"https://api.sienge.com.br/{env['cltSienge']}/public/api/v1/creditors?limit={limit}&offset={offset}"
            response = client.get(url)
            if response.status_code != 200:
                logger.logMsgError(f"Erro: {response.status_code} ")
                if response.status_code == 429: # Limite de requisições excedido
                    logger.logMsgError("Limite de requisições excedido")
                    time.sleep(10)
                    continue
                statusOK = False
                break
            for items in response.json()["results"]:
                vendas.append(items)
            qtdDados = len(response.json()["results"])
            offset = offset + qtdDados
            if qtdDados == 0:
                statusOK = False

    json_vendas = json.dumps(vendas)
    rd.set("loader_creditors", json_vendas)
    
    logger.logMsg("SERVICE SG_INTEL_VENDAS LOADER END COLLECTOR")
    

def callback(ch, method, properties, body):
    logger.logMsg("SERVICE SG_INTEL_VENDAS LOADER RUNNING")
    logger.heartBeat()
    dado = body.decode("utf-8") # "start"
    if '"' in dado:
        dado = dado[1:-1]
    logger.logMsg(f" [x] Received {dado}")

    if len(dado.split(":")) != 2:
        logger.logMsg(f"SENT CMD [{dado}] != service:status")
        return

    service, comando = dado.split(":")

    if not (service == 'dashboard' and comando == 'start'):
        logger.logMsgError(f"SERVICE {service}:{comando} UNEXPECTED")
        return

    if dashboard['atualizacoes'] <= 0:
        logger.logMsgError(f"SERVICE SG_INTEL_VENDAS LOADER atualizacoes <= 0")
        dashboard['status'] = 'Atualizado'
        rd.set("dashboard", json.dumps(dashboard))
        return
    else:
        logger.logMsg(f"SERVICE SG_INTEL_VENDAS LOADER atualizacoes: {dashboard['atualizacoes']}")

    logger.logMsg("SERVICE SG_INTEL_VENDAS LOADER STARTING")
    logger.logMsg("start")
    logger.heartBeat()
    inicio = time.time()
    try:
        collector(env)
        final = time.time() - inicio
        logger.exchange.setLogType("metric.logs")
        logger.logMsg(f"TEMPO EXTRACT:loader.{env['cltDW']}:{final}:segundos")
        logger.exchange.clrLogType()
        exchange.sendMsg(message="loader:start", rkey="intel")
        logger.logMsg("SERVICE SG_INTEL_VENDAS LOADER DONE")
        logger.logMsg("success")
        dashboard['atualizacoes'] = dashboard['atualizacoes'] - 1
        rd.set("dashboard", json.dumps(dashboard))
    except Exception as e:
        logger.logMsgError(f"SERVICE SG_INTEL_VENDAS LOADER FAILURE: {e}")
        logger.logMsg("failure")

# CONFIGURA O LOGGER
try:
    logger = LogService(system="sg_intel_vendas", service="loader", logType="logs", logLocal=True)
    logger.heartBeat()
    logger.logMsg("SERVICE SG_INTEL_VENDAS LOADER STARTING", versao)
except Exception as e:
    print("SERVICE LOGGER DOWN")
    print("Erro: " + str(e), flush=True)
    time.sleep(10)
    sys.exit(1)


# CONFIGURA O REDIS
env = {}
dashboard = {}
try:
    rd = redis.Redis("redis", 6379)
    envjs = rd.get("sg_intel_client")
    if envjs is None:
        logger.logMsgError("CHAVE sg_intel_client NAO ENCONTRADA")
        time.sleep(10)
        sys.exit(1)
    env = json.loads(envjs)
    dashboardjs = rd.get("dashboard").decode("utf-8")
    if dashboardjs is None:
        logger.logMsgError("CHAVE dashboard NAO ENCONTRADA")
        time.sleep(10)
        sys.exit(1)
    dashboard = json.loads(dashboardjs)

    logger.client = env["cltDW"]
    logger.logMsg(f"SERVICE CONFIGURED FOR CLIENT {env['cltDW']}")
except Exception as e:
    logger.logMsgError("SERVICE REDIS DOWN", e)
    time.sleep(10)
    sys.exit(1)

# CONFIGURE EXCHANGE
try:
    logger.logMsg("CONNECTING TO EXCHANGE")
    exchange = QueueExchange(service="loader", rabbit_callback=callback)
except Exception as e:
    logger.logMsgError("SERVICE RABBITMQ DOWN", e)
    time.sleep(10)
    sys.exit(1)


logger.logMsg("SERVICE SG_INTEL_VENDAS LOADER STARTED")
exchange.start_consuming()
