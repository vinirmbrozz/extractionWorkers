import json, sys, time

import redis
from pydantic import ValidationError

from Logger import LogService
from Exchange import QueueExchange

versao = "v1.00 01/12/23 Thiago - Versão inicial"
versao = "v1.01 16/12/23 MJ. Adequação novos Logger Exchange"

trilha ="vendas_units_estoque_empresa"

def start():
    lista = []
    try:
        try:
            unidades = json.loads(rd.get("Dashboard-vendas_units_estoque_empreendimento").decode())
            empresas = json.loads(rd.get("loader_vendas_companies").decode())
        except Exception as e:
            logger.logMsgError(f"Erro ao executar rd: {e}")

        for empresa in empresas:
            for unidade in unidades:
                if unidade['companyId'] == empresa['id']:
                    # Search for the company in the list
                    found = False
                    for item in lista:
                        if item["id"] == unidade['companyId']:
                            item["unidades"].append(unidade)
                            found = True
                            break
                    # If the company was not found in the list, add it
                    if not found:
                        lista.append({
                            "id": empresa['id'],
                            "name": empresa['name'],
                            "unidades": [unidade]
                        })
        rd.set("Dashboard-vendas_units_estoque_empresa", json.dumps(lista))
        logger.logMsg("FIM vendas_units_estoque_empresa")
    except Exception as e:
        logger.logMsgError(f"Erro ao consultar: ", e)
        raise ValueError("Erro ao tratar os dados do vendas_units_estoque_empresa")
    

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
        exchange.sendMsg(message=f"{trilha}:failure", rkey="intel")


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