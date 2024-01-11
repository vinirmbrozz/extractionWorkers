import redis
import json
import pandas as pd
from datetime import datetime
from Logger import LogService
from QueueExchange import QueueExchange
import sys
import time
import calendar


# SEPARA O VGV, VALOR TOTAL DE CADA EMPREENDIMENTO
# VGV POR EMPREENDIMENTO

versao = "v1.01 Tiago F. 24/11/2023 trilha concluida"
versao = "v1.02 Tiago F. 01/12/2023 ajustando padroes de execucao"
trilha = "trilha_vgvempreend"


def start(log: LogService):
    log.logMsg(f"Trilha {trilha} comecou a processar")
    vgv_L = json.loads(r.get("loader_sales_contracts_emitido").decode())
    vgv_C = json.loads(r.get("loader_sales_contracts_cancelado").decode())

    vgv_p_empreend = vgv_L + vgv_C

    df = pd.DataFrame(vgv_p_empreend)
    saida = {}
    for valor in vgv_p_empreend:
        for CA in valor["paymentConditions"]:
            isAssociative = False
            if CA["conditionTypeId"] == "CA":
                isAssociative = True
                break
        if isAssociative == False:
            resultado = df.groupby("enterpriseName")["value"].sum().reset_index()
            saida = resultado.to_json(orient="records")
    r.set(f"Dashboard-{trilha}", str(saida))
    r.close()


def callback(ch, method, properties, body):
    try:
        inicio = time.time()
        start(enviaLog)
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
