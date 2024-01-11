import json, sys, time

import redis
from collections import defaultdict
from pydantic import ValidationError

from Logger import LogService
from Exchange import QueueExchange

versao = "1.00 - 01/12/2023 Baltazar - Versão Inicial"
versao = "1.01 - 07/12/2023 Baltazar - Json corrigido"
versao = "1.02 - 15/12/2023 MJ. - Adequação novos Logger Exchange"

trilha = "vendas_corretor"

def start(log: LogService):
    try:
        log.logMsg("Iniciando vgv corretor")

        contratos = json.loads(r.get("loader_sales_contracts_emitido").decode())
        corretores = json.loads(r.get("loader_creditors").decode())

        lista_corretores = []

        for contrato in contratos:
            for broker in contrato.get("brokers", []):
                corretor = next((c for c in corretores if c["id"] == broker["id"]), None)
                if corretor:
                    lista_corretores.append({
                        "name": corretor["name"],
                        "vgv": contrato["value"],
                        "year": int(contrato["contractDate"].split("-")[0]),
                        "month": int(contrato["contractDate"].split("-")[1])
                    })
        r.set(f"Dashboard-{trilha}", json.dumps(lista_corretores))

    except Exception as e:
        log.logMsgError(f"Erro ao separar Range: {e}")
        raise ValueError("Erro ao tratar os dados do vendas_vgv_corretor")


def callback(ch, method, properties, body):
    try:
        inicio = time.time()
        start(enviaLog)
        exchange.sendMsg(message="vendas_corretor:success", rkey="intel")
        fim = time.time()
        duracao = fim - inicio
        enviaLog.exchange.setLogType("metric.logs")
        enviaLog.logMsg(f"TRILHA TEMPO TOTAL:vendas_corretor:{duracao:.2f}:segundos")

    except Exception as e:
        enviaLog.logMsgError(f"Erro ao gerar vgv corretor: ", e)
        sys.stdout.flush()
        exchange.sendMsg(message="vendas_corretor:failure", rkey="intel")


# CONFIGURA O LOGGER
try:
    enviaLog = LogService(system="sg_intel_vendas", service=trilha, logLocal=True)
    enviaLog.heartBeat()
    enviaLog.logMsg(f"SERVICE SG_INTEL_VENDAS: {trilha} STARTING", versao)
except Exception as e:
    print(f"Erro ao conectar na mensageria: {e}", flush=True)
    sys.exit(1)

# CONFIGURA REDIS
env = {}
try:
    r = redis.Redis(host="redis", port=6379)
    envjs = r.get("sg_intel_client")
    if envjs is None:
        enviaLog.logMsgError("CHAVE sg_intel_client NAO ENCONTRADA")
        time.sleep(10)
        sys.exit(1)
    env = json.loads(envjs)
    enviaLog.client = env["cltDW"]
except Exception as e:
    enviaLog.logMsgError("SERVICE REDIS DOWN", e)
    time.sleep(10)
    sys.exit(1)

# CONFIGURE EXCHANGE
try:
    enviaLog.logMsg("CONNECTING TO EXCHANGE")
    exchange = QueueExchange(service=trilha, rabbit_callback=callback)
except Exception as e:
    enviaLog.logMsgError("rabbitMq offline")
    time.sleep(10)
    sys.exit(1)

enviaLog.logMsg(f"SERVICE SG_INTEL_VENDAS: {trilha} STARTED")
exchange.start_consuming()

