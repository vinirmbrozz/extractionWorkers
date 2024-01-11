import json, time, sys

import redis
from pydantic import ValidationError
from datetime import datetime

from Logger import LogService
from Exchange import QueueExchange

versao = "v1.00 - 01/12/2023 Baltazar - Versão Inicial"
versao = "v1.01 - 07/12/2023 Baltazar - Json corrigido"
versao = "v1.05 - 15/12/2023 MJ. - Adaptação novo Logger e Exchange"

trilha = "vendas_idade"

def separaRange(log: LogService):
    try:
        log.logMsg("Iniciando range de idade.")
        data = datetime.now().year
        clientes = r.get("loader_customers")
        if clientes is None:
            raise ValueError(f"Chave 'loader_customers' não encontrada")
        clientes = json.loads(clientes.decode())

        contratos = r.get("loader_sales_contracts_emitido")
        if contratos is None:
            raise ValueError(f"Chave 'loader_sales_contracts_emitido' não encontrada")
        contratos = json.loads(contratos.decode())

        retorno = []
        for contrato in contratos:
            for cli in contrato["salesContractCustomers"]:
                for cliente in clientes:
                    if cliente["id"] == cli['id']:
                        if cliente["personType"] == "Física":
                            if "birthDate" in cliente and cliente["birthDate"] is not None:
                                nascimento = int(cliente["birthDate"].split("-")[0])
                                idade = data - nascimento
                                retorno.append({"name":cliente['name'],"value":idade,"vgv":contrato["value"],"year":int(contrato["contractDate"].split("-")[0]),"month":int(contrato["contractDate"].split("-")[1])})
        r.set("Dashboard-vendas_idade", json.dumps(retorno))
        return retorno
    except Exception as e:
        log.logMsgError(f"Erro ao separar Range: ", e)
        raise ValueError(f"Erro ao tratar os dados do range idade")


def callback(ch, method, properties, body):
    try:
        inicio = time.time()
        separaRange(enviaLog)
        exchange.sendMsg(message="vendas_idade:success", rkey="intel")
        fim = time.time()
        duracao = fim - inicio
        enviaLog.exchange.setLogType("metric.logs")
        enviaLog.logMsg(f"TRILHA TEMPO TOTAL:vendas_idade:{duracao:.2f}:segundos")
        
    except Exception as e:
        enviaLog.logMsgError(f"Erro ao calcular idade: ", e)
        exchange.sendMsg(message="vendas_idade:failure", rkey="intel")


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
