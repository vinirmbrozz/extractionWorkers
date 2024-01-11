import json, sys, time

import redis

from Logger import LogService
from Exchange import QueueExchange

versao = "v1.00 - 01/12/2023 Baltazar - Versão Inicial"
versao = "v1.01 - 15/12/2023 MJ. - Adaptação novo Logger e Exchange"

trilha = "vendas_m2"

def separaRange(m2inicial, m2final, log: LogService):
    try:
        log.logMsg(f"Tratando range m²: {m2final}")

        unidades = r.get("loader_vendas_units_estoque")
        if unidades is None:
            raise ValueError(f"Chave 'loader_vendas_units_estoque' não encontrada")
        unidades = json.loads(unidades.decode())

        calcM2 = 0
        cont = 0
        lista = []
        for unidade in unidades:
            if (
                unidade["privateArea"] >= m2inicial
                and unidade["privateArea"] <= m2final
            ):
                lista.append(unidade["id"])
                calcM2 += unidade["privateArea"]
                cont += 1
        m2medio = calcM2 / cont
        log.logMsg(f"Unidades: {len(lista)}")
        log.logMsg(f"M² medio: {m2medio}")
        return m2medio, lista
    except Exception as e:
        log.logMsgError(f"Erro ao separar Range: {e}", e)
        raise ValueError(f"Erro ao separar range no m²")


def calculam2(m2medio: float, listaUnidades: list, log: LogService):
    try:
        contratos = r.get("loader_sales_contracts_emitido")
        if contratos is None:
            raise ValueError(f"Chave 'loader_sales_contracts_emitido' não encontrada")
        contratos = json.loads(contratos.decode())
        valorMediom2 = 0
        for unidade in listaUnidades:
            for contrato in contratos:
                if contrato["id"] == unidade:
                    valorMediom2 += contrato["value"]
                    break
        retorno = (valorMediom2 / len(contratos)) / m2medio
        return retorno
    except Exception as e:
        log.logMsgError(f"Erro ao calcular m2: {e}", e)
        raise ValueError(f"Erro ao calcular m²")


def callback(ch, method, properties, body):
    try:
        inicio = time.time()
        m2medio, listaUnidades = separaRange(0, 25, log=enviaLog)
        m2medio2, listaUnidades2 = separaRange(26, 50, log=enviaLog)
        m2medio3, listaUnidades3 = separaRange(51, 500, log=enviaLog)

        range25 = calculam2(m2medio, listaUnidades, log=enviaLog)
        range50 = calculam2(m2medio2, listaUnidades2, log=enviaLog)
        range500 = calculam2(m2medio3, listaUnidades3, log=enviaLog)

        guardar = {
            "ate25m²": f"R$ {range25:,.2f}",
            "ate50m²": f"R$ {range50:,.2f}",
            "ate500m²": f"R$ {range500:,.2f}",
        }
        
        r.set("Dashboard-vendas_m2", json.dumps(guardar))
        exchange.sendMsg(message="vendas_m2:success", rkey="intel")
        fim = time.time()
        duracao = fim - inicio
        enviaLog.exchange.setLogType("metric.logs")
        enviaLog.logMsg(f"TRILHA TEMPO TOTAL:vendas_m2:{duracao:.2f}:segundos")

    except Exception as e:
        enviaLog.logMsgError(f"Erro ao calcular m2: ", e)
        exchange.sendMsg(message="vendas_m2:failure", rkey="intel")

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
