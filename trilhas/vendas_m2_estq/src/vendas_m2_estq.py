import json, sys, time

import redis
from pydantic import ValidationError

from Logger import LogService
from Exchange import QueueExchange

versao = "v1.00 01/12/23  thiago - Versão inicial"
versao = "v1.01 15/12/23 MJ. Adaptação novos Logger Exchange"

trilha = "vendas_m2_estq"

def separaRange(m2inicial, m2final):
    try:
        logger.logMsg(f"Tratando range: {m2final}")

        try:
            unidades = json.loads(rd.get("loader_vendas_units_estoque").decode())
        except ValidationError as e:
            logger.logMsgError(f"Erro ao validar modelo de unidades estoque: ", e)
            print(e)

        calcM2 = 0
        cont = 0
        lista = []
        for unidade in unidades:
            if (
                unidade["privateArea"] >= m2inicial
                and unidade["privateArea"] <= m2final
                and unidade["commercialStock"] == "V"
            ):
                lista.append(unidade["enterpriseId"])
                calcM2 += unidade["privateArea"]
                cont += 1
        m2medio = calcM2 / cont
        logger.logMsg(f"M² medio: {m2medio}")
        return m2medio, lista
    except Exception as e:
        logger.logMsgError(f"Erro ao separar Range: ", e)
        print(e)


def calculam2(m2medio: float, listaUnidades: list, ):
    try:
        try:
            contratos = json.loads(rd.get("loader_sales_contracts_emitido").decode())
        except ValidationError as e:
            logger.logMsgError(f"Erro ao validar modelo de m2 estoq: ", e)
            print(e)

        valorMediom2 = 0
        for unidade in listaUnidades:
            for contrato in contratos:
                if contrato["enterpriseId"] == unidade:
                    valorMediom2 += contrato["value"]
                    break
        retorno = (valorMediom2 / len(contratos)) / m2medio
        return retorno
    except Exception as e:
        logger.logMsgError(f"Erro ao calcular m2_estoq: ", e)
        print(e)


def start():
    try:
        m2medio, listaUnidades = separaRange(0, 25)
        m2medio2, listaUnidades2 = separaRange(26, 50)
        m2medio3, listaUnidades3 = separaRange(51, 500)

        range25 = calculam2(m2medio, listaUnidades)
        range50 = calculam2(m2medio2, listaUnidades2)
        range500 = calculam2(m2medio3, listaUnidades3)

        guardar = {
            "ate25m²": f"R$ {range25:,.2f}",
            "ate50m²": f"R$ {range50:,.2f}",
            "ate500m²": f"R$ {range500:,.2f}",
        }
        print(guardar)
        sys.stdout.flush()
        rd.set("Dashboard-vendas_m2_estq", json.dumps(guardar))
        logger.logMsg("FIM vendas_m2_estoq")

    except Exception as e:
        logger.logMsgError(f"Erro ao calcular m2_estoq: ", e)
        print(e)
        sys.stdout.flush()
    

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
        logger.logMsg(f"TEMPO API:trilha:{final}:segundos")
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


logger.logMsg(f"SERVICE SG_INTEL_VENDAS {trilha} STARTED")
exchange.start_consuming()
