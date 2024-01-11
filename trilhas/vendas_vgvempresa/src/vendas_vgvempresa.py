import json, sys, time

import redis
from pydantic import BaseModel
import pandas as pd
from datetime import datetime

from Logger import LogService
from Exchange import QueueExchange

# SEPARA O VGV, VALOR TOTAL DE CADA EMPREENDIMENTO
versao = "v1.01 24/11/2023 Tiago F. trilha concluida"
versao = "v1.02 01/12/2023 Tiago F. ajustando a"
versao = "v1.03 04/12/2023 MJ. pequenos ajustes"
versao = "v1.04 13/12/2023 Tiago F. colocando vgv por empresa por mes"
versao = "v1.05 15/12/2023 MJ. Adaptação novos Logger Exchange"

trilha = "vendas_vgvempresa"

# VGV POR EMPREENDIMENTO

def start():
    #  enviaLog.logMsg(f"SERVICE SG_INTEL_VENDAS: {trilha} RUNNING")
    vgv_L = json.loads(r.get("loader_sales_contracts_emitido").decode())
    vgv_C = json.loads(r.get("loader_sales_contracts_cancelado").decode())
    vgv_p_empresa = vgv_L + vgv_C

    df = pd.DataFrame(vgv_p_empresa)
    saida = {}
    for valor in vgv_p_empresa:
        for CA in valor["paymentConditions"]:
            isAssociative = False
            if CA["conditionTypeId"] == "CA":
                isAssociative = True
                break
        if isAssociative == False:
            dfDashboard = pd.DataFrame(vgv_p_empresa)
            resultadoDashboard = (
                dfDashboard.groupby("companyName")["value"].sum().reset_index()
            )
            saida = resultadoDashboard.to_json(orient="records")

            df["contractDate"] = pd.to_datetime(df["contractDate"])
            df["month"] = df["contractDate"].dt.month
            df["year"] = df["contractDate"].dt.year

            df = df.sort_values(by=["companyName", "year", "month"])

            df["value_acumulado"] = df.groupby("companyName")["value"].cumsum()

            resultado = (
                df.groupby(["companyName", "year", "month"])[
                    ["value", "value_acumulado"]
                ]
                .agg({"value": "sum", "value_acumulado": "max"})
                .reset_index()
            )
            saidaDataSet = resultado.to_json(orient="records")

    print(saidaDataSet)
    r.set(f"Dataset-{trilha}", str(saidaDataSet))
    r.set(f"Dashboard-{trilha}", str(saida))
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
        enviaLog.logMsgError(f"Erro No callback: ", e)
        sys.stdout.flush()
        exchange.sendMsg(message=f"{trilha}:failure", rkey="intel")


# CONFIGURA O LOGGER
try:
    enviaLog = LogService(system="sg_intel_vendas", service=trilha, logLocal=True)
    enviaLog.heartBeat()
    enviaLog.logMsg(f"SERVICE SG_INTEL_VENDAS: {trilha} STARTING", versao)
except Exception as e:
    print("SERVICE LOGGER DOWN")
    print("Erro: " + str(e), flush=True)
    time.sleep(10)
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
    enviaLog.logMsg(f"SERVICE CONFIGURED FOR CLIENT {env['cltDW']}")
except Exception as e:
    enviaLog.logMsgError("SERVICE REDIS DOWN", e)
    time.sleep(10)
    sys.exit(1)

# CONFIGURE EXCHANGE
try:
    enviaLog.logMsg("CONNECTING TO EXCHANGE")
    exchange = QueueExchange(service=trilha, rabbit_callback=callback)
except Exception as e:
    enviaLog.logMsgError("SERVICE RABBITMQ DOWN", e)
    time.sleep(10)
    sys.exit(1)

enviaLog.logMsg(f"SERVICE SG_INTEL_VENDAS: {trilha} STARTED")
exchange.start_consuming()
