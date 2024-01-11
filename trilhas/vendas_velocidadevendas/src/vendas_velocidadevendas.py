import redis
import json
from pydantic import BaseModel
from Logger import LogService
from QueueExchange import QueueExchange
import sys
import time


versao = "v1.01 - 17/11/2023 Tiago F. Logger Exchange e Redis "
versao = "v1.02 - 28/11/2023 Tiago F. Ajustes no padrão de execução "
versao = "v1.03 - 04/12/2023 Tiago F. Trilha trazendo valores zerados quando não houver venda no mes"
versao = "v1.04 - 04/12/2023 M.J. Sequencia inicialização e cnf LogService"
trilha = "vendas_velocidadevendas"

ano_anterior = "22"
ano_posterior = "23"


class EnterpriseTratament(BaseModel):
    enterpriseName: str
    enterpriseId: int


class QtdUnit(BaseModel):
    enterpriseName: str
    enterpriseId: int
    qtnd: int


class VelocidadeVendas(BaseModel):
    enterprise: str
    date: str
    value: float


class Enterprise(BaseModel):
    empresa: list[VelocidadeVendas]


def start():
    try:
        enviaLog.logMsg("SERVICE SG_INTEL_VENDAS: vendas_vgvperiodo RUNNING")
        # CARREGAMENTO REDIS
        units = json.loads(r.get("loader_vendas_units_estoque").decode())
        contratos_emitidos = json.loads(
            r.get("loader_sales_contracts_emitido").decode()
        )
        contratos_cancelados = json.loads(
            r.get("loader_sales_contracts_cancelado").decode()
        )
        contratos = contratos_emitidos + contratos_cancelados

        # CRIACAO DE VARIAVEIS
        lista_de_empreendimentos = []
        dic_meses = {
            "01": "JAN",
            "02": "FEV",
            "03": "MAR",
            "04": "ABR",
            "05": "MAI",
            "06": "JUN",
            "07": "JUL",
            "08": "AGO",
            "09": "SET",
            "10": "OUT",
            "11": "NOV",
            "12": "DEZ",
        }

        lista_de_empreendimentos = json.loads(r.get("loader_enterprises").decode())
        # BLOCO QUE SEPARAMOS OS EMPREENDIMENTOS QUE SERAM USADOS

        lista_de_meses = []
        lista_de_units = []

        # AVALIA CADA CONTRATO POR CREDITO ASSOCIATIV0 (ATÉ O MOMENTO 30/11/2023 ESTES CONTRATOS NÃO ENTRAM NA CONTA DE QUANTIDADE EM NENHUMA DAS TRILHAS)
        for valor in contratos:
            for CA in valor["paymentConditions"]:
                isAssociative = False
                if CA["conditionTypeId"] == "CA":
                    isAssociative = True
                    break
            if isAssociative == False:
                for empreendimento in lista_de_empreendimentos:
                    qtd_unidades_vendidas = 0
                    # AVALIA CADA CONTRATO POR EMPREENDIMENTO E SEPARA OS MESES QUE HOUVERAM VENDAS NO SISTEMA, COM BASE NO DICIONARIO DE DATAS E NO CAMPO contractDate
                    if empreendimento["enterpriseId"] == valor["enterpriseId"]:
                        if valor["contractDate"].split("-")[1] in dic_meses:
                            for i in range(len(lista_de_meses)):
                                data = f'{dic_meses[valor["contractDate"].split("-")[1]]}{valor["contractDate"].split("-")[0][2:4]}'
                                # SE O CONTRATO JA PASSOU POR AQUI NAQUELE MES, ELE ADICIONA UMA VENDA
                                if (
                                    data == lista_de_meses[i]["date"]
                                    and empreendimento["enterpriseName"]
                                    == lista_de_meses[i]["enterprise"]
                                ):
                                    # SE O CONTRATO NUNCA PASSOU POR AQUI, ADICIONA UMA VENDA
                                    lista_de_meses[i]["value"] += 1
                                    break

                            #  AQUI SE MONTA O DICIONARIO COM AS VENDAS POR DATA DE CADA MES VENDIDO, ESTE ELSE, ESTA PARA FOR DE CADA MES QUE HOUVE VENDAS
                            else:
                                qtd_unidades_vendidas += 1
                                lista_de_meses.append(
                                    VelocidadeVendas(
                                        enterprise=valor["enterpriseName"],
                                        date=f'{dic_meses[valor["contractDate"].split("-")[1]]}{valor["contractDate"].split("-")[0][2:4]}',
                                        value=qtd_unidades_vendidas,
                                    ).model_dump()
                                )
        # AQUI EU CONTO QUANTAS UNIDADES TEM CADA EMPREENDIMENTO
        lista_de_velocidade = []

        # AQUI A CONTA É FEITA COM BASE NAS UNIDADES POR EMPREENDIMENTO E NO QUANTIDADE DE VENDAS POR MES
        for i in range(len(lista_de_empreendimentos)):
            quantidade = lista_de_empreendimentos[i]["qtyUnits"]
            for valor_empreend in lista_de_meses:
                if (
                    valor_empreend["enterprise"]
                    == lista_de_empreendimentos[i]["enterpriseName"]
                ):
                    porcentagem = valor_empreend["value"] / quantidade
                    lista_de_velocidade.append(
                        {
                            "nome": valor_empreend["enterprise"],
                            "data": valor_empreend["date"],
                            "valor": porcentagem,
                        }
                    )
                    quantidade -= valor_empreend["value"]
        monthOrder = [
            "JAN",
            "FEV",
            "MAR",
            "ABR",
            "MAI",
            "JUN",
            "JUL",
            "AGO",
            "SET",
            "OUT",
            "NOV",
            "DEZ",
        ]
        for empreend in lista_de_empreendimentos:
            for meses in monthOrder:
                for i in range(len(lista_de_velocidade)):
                    if (
                        meses not in lista_de_velocidade[i]["data"].replace("22", "")
                        and empreend["enterpriseName"] == lista_de_velocidade[i]["nome"]
                    ):
                        lista_de_velocidade.append(
                            {
                                "nome": empreend["enterpriseName"],
                                "data": f"{meses}{ano_anterior}",
                                "valor": 0,
                            }
                        )
                        break
                    else:
                        continue

        for empreend in lista_de_empreendimentos:
            for meses in monthOrder:
                for i in range(len(lista_de_velocidade)):
                    if (
                        meses not in lista_de_velocidade[i]["data"].replace("23", "")
                        and empreend["enterpriseName"] == lista_de_velocidade[i]["nome"]
                    ):
                        lista_de_velocidade.append(
                            {
                                "nome": empreend["enterpriseName"],
                            "data": f"{meses}{ano_posterior}",
                                "valor": 0,
                            }
                        )
                        break
                    else:
                        continue
        lista_de_remocao = []
        for out23 in lista_de_velocidade:
            for i in range(len(lista_de_meses)):
                if (
                    lista_de_meses[i]["date"] == out23["data"]
                    and out23["nome"] == lista_de_meses[i]["enterprise"]
                    and out23["valor"] == 0
                ):
                    lista_de_remocao.append(out23)
                    break

        lista_resultante = [item for item in lista_de_velocidade if item not in lista_de_remocao]

        print(lista_resultante)

        r.set(f"Dashboard-{trilha}", json.dumps(lista_resultante))
        r.close()
        enviaLog.logMsg("SERVICE SG_INTEL_VENDAS: vendas_vgvperiodo DONE")
    except Exception as e:
        enviaLog.logMsgError("Erro as consultar: ", e)
        raise ValueError(f"Erro ao tratar os dados do vendas_velocidadevendas: {e}")


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
        exchange.sendMsg(message=f"{trilha}:failure", rkey="intel")


# CONFIGURA O LOGGER
try:
    enviaLog = LogService(system="sg_intel_vendas", service=trilha, logLocal=True)
    enviaLog.heartBeat()
    enviaLog.logMsg("SERVICE SG_INTEL_VENDAS: vendas_vgvperiodo STARTING", versao)
except Exception as e:
    print("SERVICE LOGGER DOWN")
    print("Erro: " + str(e), flush=True)
    time.sleep(10)
    sys.exit(1)

# CONFIGURA O REDIS
# DADOS CONFIGURAÇÃO REDIS
env = {}
try:
    r = redis.Redis("redis", 6379)
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

# CONFIGURA O EXCHANGE
try:
    enviaLog.logMsg("CONNECTING TO EXCHANGE")
    exchange = QueueExchange(service=trilha, rabbit_callback=callback)
except Exception as e:
    enviaLog.logMsgError("SERVICE RABBITMQ DOWN", e)
    time.sleep(10)
    sys.exit(1)


enviaLog.logMsg("SERVICE SG_INTEL_VENDAS vendas_velocidadevendas STARTED")
exchange.start_consuming()
