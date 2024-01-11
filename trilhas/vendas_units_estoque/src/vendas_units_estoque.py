import json, sys, time

import redis
from pydantic import ValidationError

from Logger import LogService
from Exchange import QueueExchange

versao = "v1.00 01/12/23 Thiago - Versão inicial"
versao = "v1.01 15/12/23 MJ. Adequação novos Logger Exchange"

trilha = "vendas_units_estoque"

def start():
    siglas = {
        "D": "Disponível",
        "V": "Vendida",
        "L": "Locada",
        "C": "Reservada",
        "R": "Reserva Técnica",
        "E": "Permuta",
        "M": "Mutuo",
        "P": "Proposta",
        "T": "Transferido",
        "G": "Vendido/Terceiros",
        "O": "Vendida em pré-contrato",
    }
    dictEstoque = {}
    try:
        try:
            unidades = json.loads(rd.get("loader_vendas_units_estoque").decode())
        except Exception as e:
            logger.logMsgError(f"Erro ao executar rd: {e}")

        for unidade in unidades:
            if siglas[unidade["commercialStock"]] in dictEstoque:
                dictEstoque[siglas[unidade["commercialStock"]]] += 1
            else:
                dictEstoque[siglas[unidade["commercialStock"]]] = 1

        rd.set("Dashboard-vendas_units_estoque", json.dumps(dictEstoque))
        logger.logMsg("FIM vendas_units_estoque")
    except Exception as e:
        logger.logMsgError("Erro ao consultar: ", e)
        raise ValueError(f"Erro ao tratar os dados do vendas_units_estoque")


def callback(ch, method, properties, body):
    logger.logMsg(f"SERVICE SG_INTEL_VENDAS {trilha} RUNNING")
    logger.heartBeat()
    logger.logMsg(" [x] Received %r" % body)
    inicio = time.time()
    try:
        start()
        logger.logMsg(f"SERVICE SG_INTEL_VENDAS {trilha} DONE")
        exchange.sendMsg(message=f"{trilha}:success", rkey="intel")
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
    logger.logMsg(f"SERVICE SG_INTEL_VENDAS {trilha} STARTING", versao)
except Exception as e:
    print("SERVICE LOGGER DOWN", e)
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
    logger.logMsgError("SERVICE REDIS DOWN", e)
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


logger.logMsg(f"SERVICE SG_INTEL_VENDAS {trilha} STARTED")
exchange.start_consuming()
