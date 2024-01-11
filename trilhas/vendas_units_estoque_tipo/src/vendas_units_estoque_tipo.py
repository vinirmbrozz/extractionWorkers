
import redis
import json
from pydantic import ValidationError
from Logger import LogService
from QueueExchange import QueueExchange
import sys
import time
versao = "1.0 01/12/23  thiago - Versão inicial"
trilha = "vendas_units_estoque_tipo"

def start():
    lista = []
    contadores = []
    try:
        try:
                unidades = json.loads(rd.get("loader_vendas_units_estoque").decode())
        except Exception as e:
            logger.logMsgError(f"Erro ao executar rd: {e}")

        for unidade in unidades:
            property_type = unidade['propertyType'].strip().upper()

            encontrado = False
            for item in lista:
                if item["propertyType"] == property_type:
                    # Se encontrado, incrementa a contagem
                    item["count"] += 1
                    encontrado = True
                    break

            # Se não encontrado, adiciona à lista com contagem inicial em 1
            if not encontrado:
                data = unidade["deliveryDate"].split("-")
                lista.append({
                    "propertyType": property_type,
                    "count": 1
                })
        rd.set("Dashboard-vendas_units_estoque_tipo", json.dumps(lista))
        logger.logMsg("FIM estoque vendas_units_estoque_tipo")
           
    except Exception as e:
        logger.logMsgError("Erro ao consultar: ", e)
        raise ValueError(f"Erro ao tratar os dados do vendas_units_estoque_tipo")
    

def callback(ch, method, properties, body):
    logger.logMsg("SERVICE SG_INTEL_VENDAS {trilha} RUNNING")
    logger.heartBeat()
    logger.logMsg(" [x] Received %r" % body)
    inicio = time.time()
    try:
        start()
        logger.logMsg("SERVICE SG_INTEL_VENDAS {trilha} DONE")
        exchange.sendMsg(message="start", rkey="intel")
        final = time.time() - inicio
        logger.exchange.setLogType("metric.logs")
        logger.logMsg(f"TEMPO API:{trilha}:{final}:segundos")
        logger.exchange.clrLogType()
    except Exception as e:
        logger.logMsgError(f"Erro ao executar {trilha}: {e}")


# CONFIGURA O LOGGER
try:
    logger = LogService(
        system="sg_intel_vendas", service=trilha, logType="logs", logLocal=True
    )
    logger.heartBeat()
    logger.logMsg("SERVICE SG_INTEL_VENDAS {trilha} STARTING", versao)
except Exception as e:
    logger.logMsgError("SERVICE LOGGER DOWN", e)
    time.sleep(10)
    sys.exit(1)


# CONFIGURA O REDIS
# DADOS CONFIGURAÇÃO REDIS
env = {}
try:
    rd = redis.Redis("redis", 6379)
    envjs = rd.get("sg_intel_client")
    if envjs is None:
        logger.logMsgError("CHAVE sg_intel_client NAO ENCONTRADA")
        time.sleep(10)
        sys.exit(1)
    env = json.loads(envjs)
    logger.client = env["cltDW"]
    logger.logMsg(f"SERVICE CONFIGURED FOR CLIENT {env['cltDW']}")
except Exception as e:
    print("SERVICE REDIS DOWN", e)
    time.sleep(10)
    sys.exit(1)

# CONFIGURE EXCHANGE
try:
    logger.logMsg("CONNECTING TO EXCHANGE")
    exchange = QueueExchange(service=trilha, rabbit_callback=callback)
except Exception as e:
    logger.logMsgError("Erro ao conectar na mensageria", e)
    time.sleep(10)
    sys.exit(1)


logger.logMsg("SERVICE SG_INTEL_VENDAS {trilha} STARTED")
exchange.start_consuming()