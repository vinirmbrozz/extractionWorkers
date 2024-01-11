
import json, sys, time

import redis
from pydantic import ValidationError

from Logger import LogService
from Exchange import QueueExchange

versao = "v1.00 01/12/23 Thiago - Versão inicial"
versao = "v1.01 16/12/23 MJ. - Adequação novos Logger Exchange"

trilha = "vendas_units_estoque_empreendimento"

def start():
    lista = []
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
    property_type = []
    try:
        try:
            unidades = json.loads(rd.get("loader_vendas_units_estoque").decode())
            enterprises = json.loads(rd.get("loader_enterprises").decode())
        except Exception as e:
            logger.logMsgError(f"Erro ao executar rd: {e}")
        
        property_type = [ty['enterpriseId'] for ty in enterprises]

        # print("AQUI ESTA O DADO", property_type)
        sys.stdout.flush()

        for unidade in unidades:
            encontrado = False
            for item in lista:
                if item['enterpriseId'] == unidade["enterpriseId"]:
                    # Se encontrado, incrementa a contagem
                    if siglas[unidade["commercialStock"]] in item["tipos"]:
                        item["tipos"][siglas[unidade["commercialStock"]]] += 1
                    else:
                        item["tipos"][siglas[unidade["commercialStock"]]] = 1
                    item["total"] += 1
                    encontrado = True
                    break
 
            # Se não encontrado, adiciona à lista com contagem inicial em 1
            if not encontrado and unidade["enterpriseId"] in property_type:
                encontrado = True
                data = unidade["deliveryDate"].split("-") if unidade["deliveryDate"] else ["", ""]
                # print(data, flush=True)
                lista.append({
                    "enterpriseId": unidade["enterpriseId"],
                    "name": "",
                    "companyId": "",
                    "property_type": unidade["propertyType"].strip().upper(),
                    "deliveryDate": unidade["deliveryDate"],
                    "mes": data[1],
                    "ano": data[0],
                    "tipos": {
                        siglas[unidade["commercialStock"]]: 1
                    },
                    "total": 1
                })
        return lista   
           
    except Exception as e:
        logger.logMsgError("Erro ao consultar: ", e)
        raise ValueError(f"Erro ao tratar os dados do vendas_estoque_empreendimento")

def getName():
    try:
        listaName = start()
        unidades = json.loads(rd.get("loader_vendas_enterprises").decode())
        for unidade in unidades:
            for item in listaName:
                if unidade["id"] == item["enterpriseId"]:
                    item["name"] = unidade["name"]
                    item["companyId"] = unidade["companyId"]
        print(listaName)

        rd.set("Dashboard-vendas_units_estoque_empreendimento", json.dumps(listaName))
        logger.logMsg("FIM venda_units_estoque_empreendimento")
    except Exception as e:
        logger.logMsgError("Erro ao consultar: ", e)
        raise ValueError(f"Erro ao tratar os dados do vendas_units_estoque_empreendimento")

def callback(ch, method, properties, body):
    logger.logMsg(f"SERVICE SG_INTEL_VENDAS {trilha} RUNNING")
    logger.heartBeat()
    logger.logMsg(" [x] Received %r" % body)
    inicio = time.time()
    try:
        getName()
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