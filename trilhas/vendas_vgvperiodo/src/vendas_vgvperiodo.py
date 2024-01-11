import json, sys, time, calendar

import redis
import pandas as pd
from datetime import datetime

from Logger import LogService
from Exchange import QueueExchange

# SEPARA O VGV, VALOR TOTAL DE CADA EMPREENDIMENTO

# VGV POR PERIODO
# AUTOR: TIAGO CARVALHO

versao = "v1.01 - 17/11/2023 Logger Exchange e Redis "
versao = "v1.02 - 28/11/2023 Ajustes no padrão de execução "
versao = "v1.03 - 01/12/2023 MJ - Ajustes no padrão de execução"
versao = "v1.04 - 01/12/2023 Tiago F. Parametrização de valores globais"
versao = "v1.05 - 15/12/2023 MJ - Adequação novos Logger Exchange"

dashboard_1 = "Dashboard-vendas_vgvperiodo"
trilha = "vendas_vgvperiodo"
trilha_2 = "vendas_ticketmedio"


def start():
    try:
        enviaLog.logMsg(f"SERVICE SG_INTEL_VENDAS: {trilha} RUNNING")

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

                pd.DataFrame(
                    {
                        "year": [datetime.now().year - 1] * 12
                        + [datetime.now().year] * 12,
                        "month": list(range(1, 13)) * 2,
                    }
                )
                resultado = df.groupby(["year", "month"])["value"].sum().reset_index()
                saida = resultado.to_json(orient="records")

        r.set(f"Dashboard-{trilha}", str(saida))
        enviaLog.logMsg(f"FIM {trilha}")

        vgv_P = json.loads(r.get(f"Dashboard-{trilha}").decode())
        i = 0
        for valor in vgv_P:
            ultimo_dia = calendar.monthrange(valor["year"], valor["month"])[1]
            resultado = valor["value"] / ultimo_dia
            valor["value"] = resultado
        r.set(f"Dashboard-{trilha_2}", str(vgv_P))
        r.close()
        enviaLog.logMsg(f"SERVICE SG_INTEL_VENDAS: {trilha} DONE")

    except Exception as e:
        enviaLog.logMsgError("Erro as consultar: ", e)
        raise ValueError(f"Erro ao tratar os dados do vendas_vgvperiodo: {e}")


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
