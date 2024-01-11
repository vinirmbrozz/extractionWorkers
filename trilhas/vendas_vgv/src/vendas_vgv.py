import json, sys, time

import redis
import pandas as pd
from datetime import datetime
import calendar

from Logger import LogService
from Exchange import QueueExchange

# SEPARA O VGV, VALOR TOTAL DE TODOS OS EMPREENDIMENTOS
# EMITIDO
versao = "v1.01 24/11/2023 Tiago F. trilha concluida"
versao = "v1.02 - 01/12/2023 Tiago F.Parametros de execucao"
versao = "v1.03 - 04/12/2023 M.J. Ajuste execução"
versao = "v1.04 - 15/12/2023 M.J. Adaptação novos Logger Exchange"

trilha = "vendas_vgv"


def start():
    enviaLog.logMsg(f"SERVICE SG_INTEL_VENDAS: {trilha} RUNNING")

    vgv_L = json.loads(r.get("loader_sales_contracts_emitido").decode())
    vgv_C = json.loads(r.get("loader_sales_contracts_cancelado").decode())

    totalValueL = 0
    qtd_contratos_L = 0
    for valor in vgv_L:
        for CA in valor["paymentConditions"]:
            isAssociative = False
            if CA["conditionTypeId"] == "CA":
                isAssociative = True
                break
        if isAssociative == False:
            qtd_contratos_L += 1
            totalValueL += valor["value"]

    # CANCELADO
    totalValueC = 0
    qtd_contratos_C = 0
    for valor in vgv_C:
        for CA in valor["paymentConditions"]:
            isAssociative = False
            if CA["conditionTypeId"] == "CA":
                isAssociative = True
                break
        if isAssociative == False:
            qtd_contratos_C += 1
            totalValueC += valor["value"]

    # BRUTO
    totalValueB = totalValueC + totalValueL
    qtd_contrato_B = qtd_contratos_C + qtd_contratos_L

    # MONTANDO JSON

    valores = {
        "trilhas_vendas_vgv": {
            "vgv_liquido": {"value": totalValueL, "qty": qtd_contratos_L},
            "vgv_cancelado": {"value": totalValueC, "qty": qtd_contratos_C},
            "vgv_bruto": {"value": totalValueB, "qty": qtd_contrato_B},
        }
    }

    r.set(f"Dashboard-{trilha}", str(json.dumps(valores)))
    r.close()
    enviaLog.logMsg(f"SERVICE SG_INTEL_VENDAS: {trilha} DONE")



def callback(ch, method, properties, body):
    try:
        inicio = time.time()
        start()
        exchange.sendMsg(message=f"{trilha}:success", rkey="intel")
        fim = time.time()
        duracao = fim - inicio
        enviaLog.exchange.setLogType("metric.logs")
        enviaLog.logMsg(f"TRILHA TEMPO TOTAL:{trilha}:{duracao:.2f}:segundos")

    except Exception as e:
        enviaLog.logMsgError(f"Erro ao callback: ", e)
        sys.stdout.flush()
        exchange.sendMsg(message=f"{trilha}:failure", rkey="intel")


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
