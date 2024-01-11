import sys, time, json

import redis
from unidecode import unidecode

from Logger import LogService
from Exchange import QueueExchange

versao = "1.00 - 01/12/2023 Baltazar - Versão Inicial"
versao = "1.01 - 15/12/2023 MJ. - Adequação novos Logger Exchange"

trilha = "vendas_estoque_uf"

def agrupar(log: LogService):
    try:
        log.logMsg("Iniciando processamento vendas estoque UF")
        municipios = r.get("loader_municipios")
        if municipios is None:
            raise ValueError(f"Chave 'loader_municipios' não encontrada")
        municipios = json.loads(municipios.decode())

        contratos = r.get("loader_sales_contracts_emitido")
        if contratos is None:
            raise ValueError(f"Chave 'loader_sales_contracts_emitido' não encontrada")
        contratos = json.loads(contratos.decode())

        empreendimentos = r.get("loader_vendas_enterprises")
        if empreendimentos is None:
            raise ValueError(f"Chave 'loader_vendas_enterprises' não encontrada")
        empreendimentos = json.loads(empreendimentos.decode())

        dict_uf = {}
        for contrato in contratos:
            for empreendimento in empreendimentos:
                if contrato["enterpriseId"] == empreendimento["id"]:
                    for municipio in municipios.keys():
                        busca = unidecode(empreendimento["adress"])
                        if municipio in busca.lower():
                            if municipios[municipio] in dict_uf:
                                dict_uf[municipios[municipio].upper()] += 1
                            else:
                                dict_uf[municipios[municipio].upper()] = 1

        r.set("Dashboard-vendas_estoque_uf", json.dumps(dict_uf))
        log.logMsg("FIM estoque uf")
    except Exception as e:
        log.logMsgError("Erro ao consultar: ", e)
        raise ValueError(f"Erro ao tratar os dados do vendas_estoque_uf")


def callback(ch, method, properties, body):
    try:
        inicio = time.time()
        agrupar(enviaLog)
        exchange.sendMsg(message="vendas_estoque_uf:success", rkey="intel")
        fim = time.time()
        duracao = fim - inicio
        enviaLog.exchange.setLogType("metric.logs")
        enviaLog.logMsg(f"TRILHA TEMPO TOTAL:vendas_estoque_uf:{duracao:.2f}:segundos")
    except Exception as e:
        enviaLog.logMsgError(f"Erro ao callback: ", e)
        sys.stdout.flush()
        exchange.sendMsg(message="vendas_estoque_uf:failure", rkey="intel")


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
