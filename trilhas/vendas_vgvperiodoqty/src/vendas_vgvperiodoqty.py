import redis
import json
import pandas as pd
from datetime import datetime
from Logger import LogService
from Exchange import QueueExchange
import sys
import time
import calendar

# SEPARA O VGV, VALOR TOTAL DE CADA EMPREENDIMENTO

# VGV POR EMPREENDIMENTO
versao = "v1.01 Tiago F. padronizando parametros de execucao"
versao = "v1.02 Tiago F. 11/01/2024 trilha ajustada, estava dando erro na ticket medio, foi adicionado o json.loads na ultima chave"
trilha = "vendas_vgvperiodoqty"
trilha_2 = "vendas_ticketmedioqty"


def start(log: LogService):
    vgv_L = json.loads(r.get("loader_sales_contracts_emitido").decode())
    vgv_C = json.loads(r.get("loader_sales_contracts_cancelado").decode())

    vgv_p_periodo = vgv_L + vgv_C

    df = pd.DataFrame(vgv_p_periodo)
    saida = {}
    for valor in vgv_p_periodo:
        for CA in valor["paymentConditions"]:
            isAssociative = False
            if CA["conditionTypeId"] == "CA":
                isAssociative = True
                break
        if isAssociative == False:
            df["contractDate"] = pd.to_datetime(df["contractDate"])
            df["month"] = df["contractDate"].dt.month
            df["year"] = df["contractDate"].dt.year
            # Extract year and month from "contractDate"

            all_months = pd.DataFrame(
                {
                    "year": [datetime.now().year - 1] * 12 + [datetime.now().year] * 12,
                    "month": list(range(1, 13)) * 2,
                }
            )
            # Merge dataframes
            resultado = df.groupby(["year", "month"])["value"].count().reset_index()
            saida = resultado.to_json(orient="records")
            i = 0
            r.set(f"Dashboard-{trilha}", str(saida))
            vgv_P = json.loads(r.get(f"Dashboard-{trilha}").decode())

    for valor in vgv_P:
        ultimo_dia = calendar.monthrange(valor["year"], valor["month"])[1]
        resultado = valor["value"] / ultimo_dia
        valor["value"] = resultado
    r.set(f"Dashboard-{trilha_2}", str(vgv_P))

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
