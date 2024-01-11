import json,sys,time

import redis
import pydantic

from Logger import LogService
from Exchange import QueueExchange 

versao = "v1.00 Intel Versao Inicial"
versao = "v1.01 Coordenação das Trilhas"
versao = "v1.02 - 30/11/2023 Envio de messagem entre processos com 'serviço:msg'"
versao = "v1.03 - 03/12/2023 Envio de messagem entre processos com 'serviço:msg' e 'trilha:msg'" 
versao = "v1.04 - M.J. 15/12/23 - Adaptação novo Logger e Exchange"

# ACIONAMENTO TRILHAS
def acionarTrilhas():
    inicio = time.time()
    date = time.strftime("%Y-%m-%d %H:%M:%S")
    rd.set("loader_inicio",inicio)
    for trilha in Trilhas_Vendas:
        logger.logMsg(f"Acionando Trilha: {trilha}")
        Trilhas_Vendas[trilha]["status"]= "start"
        Trilhas_Vendas[trilha]["date"] = date
        exchange.sendMsg(message="start", rkey=trilha)
    
    logger.heartBeat()

def setStatusTrilhas(trilha, status):
    logger.logMsg(f"Trilha: {trilha} Status: {status}")
    date = time.strftime("%Y-%m-%d %H:%M:%S")

    if status not in ["success", "failure"]:
        status = "undetermined"
        
    Trilhas_Vendas[trilha]["status"] = status
    Trilhas_Vendas[trilha]["date"] = date

def checkUpdateDashboard():
    date = time.strftime("%Y-%m-%d %H:%M:%S")
    toUpdateDashboard = True
    for trilha in Trilhas_Vendas:
        if Trilhas_Vendas[trilha]["status"] != 'success':
            toUpdateDashboard = False
            break

    if toUpdateDashboard:
        dashboardjs = rd.get("dashboard").decode("utf-8")
        dashboard = json.loads(dashboardjs)
        dashboard['status'] = 'Atualizado'
        dashboard['data'] = date
        rd.set("dashboard", json.dumps(dashboard))
        inicio = float(rd.get("loader_inicio").decode("utf-8"))
        final = time.time() - inicio
        logger.logMsg(f"TEMPO LOGGER:intel:{final}:segundos")
        logger.logMsg("SERVICE SG_INTEL_VENDAS INTEL DONE: {Trilhas_Vendas}")
        rd.delete("loader_inicio")
        
def callback(ch, method, properties, body):
    logger.logMsg("SERVICE SG_INTEL_VENDAS INTEL RUNNING")
    logger.heartBeat()
    dado = body.decode("utf-8") # "start"
    if '"' in dado:
        dado = dado[1:-1]
    logger.logMsg(f" [x] Received {dado}")

    if len(dado.split(":")) != 2:
        logger.logMsg(f"SENT CMD [{dado}] != service:status")
        return

    trilha, status = dado.split(":")
    logger.logMsg(f"Source: {trilha} Status: {status}" )

    if  trilha == 'loader':
        if status == 'start':
            rd.set("waitLoadingTrilhas", "True")
            acionarTrilhas()
            rd.delete("waitLoadingTrilhas")
        else:
            logger.logMsgError(f"Loader Status {status} Nao esperado")
        return
    
    if trilha not in Trilhas_Vendas.keys():
        logger.logMsgError(f"Trilha {trilha} NAO ENCONTRADA")
        return

    setStatusTrilhas(trilha, status)
    if not rd.get("waitLoadingTrilhas"):
        checkUpdateDashboard()

# INTEGRACAO LOGGER
try:
    logger = LogService(system="sg_intel_vendas", service="intel", logLocal=True)
    logger.heartBeat()
    logger.logMsg("SERVICE SG_INTEL_VENDAS INTEL STARTING", versao)
except Exception as e:
    logger.logMsgError("SERVICE LOGGER DOWN", e)
    time.sleep(10)
    sys.exit(1)

# INTEGRACAO REDIS
env = {}
try:
    rd = redis.Redis("redis", port=6379)
    envjs = rd.get("sg_intel_client")
    if envjs is None:
        logger.logMsgError("CHAVE sg_intel_client NAO ENCONTRADA")
        time.sleep(10)
        sys.exit(1)
    env = json.loads(envjs)
    logger.client = env["cltDW"]
    logger.logMsg(f"SERVICE CONFIGURED FOR CLIENT {env['cltDW']}")
except Exception as e:
    logger.logMsgError("SERVICE REDIS DOWN", e)
    time.sleep(10)
    sys.exit(1)

# INTEGRACAO EXCHANGE
try:
    logger.logMsg("CONNECTING TO EXCHANGE")
    exchange = QueueExchange(service="intel", rabbit_callback=callback)
except Exception as e:
    logger.logMsgError("Erro ao conectar na mensageria", e)
    time.sleep(10)
    sys.exit(1)


# CARGA DAS TRILHAS
Trilhas_Vendas = {}
try:
    Trilhasjs = rd.get("intel_trilhas").decode("utf-8")
    Trilhas = json.loads(Trilhasjs)
except:
    Trilhas = []
    logger.logMsgError("CHAVE intel_trilhas NAO ENCONTRADA")
    time.sleep(10)
    sys.exit(1)

date = time.strftime("%Y-%m-%d %H:%M:%S")
for trilha in Trilhas:
    Trilhas_Vendas[trilha] = {"status": "waiting", "date": date}
logger.logMsg(f"TRACKS AVAILABLE: {Trilhas_Vendas.keys()}")
trilhas_vendas = json.dumps(Trilhas_Vendas) # Trilhas_Vendas
logger.logMsg("SERVICE SG_INTEL_VENDAS INTEL STARTED")
exchange.start_consuming()