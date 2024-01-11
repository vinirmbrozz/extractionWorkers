import json, sys, time

import redis

from Logger import LogService
from Exchange import QueueExchange

versao = "v1.00 - 01/12/2023 Baltazar - Versão Inicial"
versao = "v1.00 - 15/12/2023 MJ. Adequação novos Logger Exchange"

trilha = "vendas_profissao"

def agrupar(log: LogService):
    try:
        log.logMsg("Iniciando processamento vendas profissao")

        contratos = r.get("loader_sales_contracts_emitido")
        if contratos is None:
            raise ValueError(f"Chave 'loader_sales_contracts_emitido' não encontrada")
        contratos = json.loads(contratos.decode())

        clientes = r.get("loader_customers")
        if clientes is None:
            raise ValueError(f"Chave 'loader_customers' não encontrada")
        clientes = json.loads(clientes.decode())

        listaProfissoes = []
        for contrato in contratos:
            for cli in contrato["salesContractCustomers"]:
                for cliente in clientes:
                    if cli["id"] == cliente["id"]:
                        if cliente["personType"] == "Física":
                            if cliente["profession"]:
                                listaProfissoes.append({"name": cliente["profession"], "value": contrato["value"], "year": int(contrato["contractDate"].split("-")[0]), "month": int(contrato["contractDate"].split("-")[1])})
        
        
        r.set("Dashboard-vendas_profissoes", json.dumps(listaProfissoes))
        log.logMsg("FIM vendas profissao")
    except Exception as e:
        log.logMsgError("Erro ao consultar: ", e)
        raise ValueError(f"Erro ao tratar vendas profissao")


def callback(ch, method, properties, body):
    try:
        inicio = time.time()
        agrupar(enviaLog)
        exchange.sendMsg(message="vendas_profissao:success", rkey="intel")
        fim = time.time()
        duracao = fim - inicio
        enviaLog.exchange.setLogType("metric.logs")
        enviaLog.logMsg(f"TRILHA TEMPO TOTAL:vendas_profissao:{duracao:.2f}:segundos")

    except Exception as e:
        enviaLog.logMsgError(f"Erro ao callback: ", e)
        exchange.sendMsg(message="vendas_profissao:failure", rkey="intel")

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
