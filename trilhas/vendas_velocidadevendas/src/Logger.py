import json, pprint
from datetime import datetime
import time, sys
import inspect
import pika
from pika.exceptions import StreamLostError
from pydantic import BaseModel, ValidationError
import logging

versao = "v1.0 Fecha a conexao apos envio da mensagem"
versao = "v1.1 Prioriza log local antes de envio para messageria"
versao = "v1.2 Deleta explicitamente a conexao com a messageria"

# CONFIGURACAO DO EXCHANGE
# SERVICO DE ENVIO DE LOGS
# EXCHANGE TIPO TOPIC
class Exchange:
    def __init__(self, system: str, service: str, logType: str):
        # Sistema SG_ETL - Tough Tiger - 10M msg/month, 100 conn, 1500 queues w 100k msgs
        self.url        = 'amqps://mylvcdwm:H9VEH5jbwfkbNCoZuWt3NPesIPwegn9o@jackal.rmq.cloudamqp.com/mylvcdwm'
        self.params     = pika.URLParameters(self.url)
        self.exchange   = 'topic_softgo' 
        self.system     = system
        self.service    = service
        self.logType    = logType
        self.logTypeSvd = logType         
        self.routeKey   = f"{self.logType}.{self.service}.{self.system}" # logs.hash.sg_etl.crescer, status.hash.sg_etl
        self.logTypeList= ['error.logs', 'logs', 'metric.logs', 'status']
        self.params.socket_timeout = 5 

    # Para quando desejar trocar o header do topico
    def setLogType(self, logType:str):
        if logType in self.logTypeList:
            self.logType    = logType
            self.routeKey   = f"{self.logType}.{self.service}.{self.system}" # logs.hash.sg_etl.crescer, status.hash.sg_etl
    
    # Volta ao header topico configurado no inicio
    def clrLogType(self):
        self.logType = self.logTypeSvd
        self.routeKey   = f"{self.logType}.{self.service}.{self.system}" # logs.hash.sg_etl.crescer, status.hash.sg_etl
              
    # Funcionamento: caso nao seja possivel enviar a msg, tentar nova conexao
    # e envio da msg original mais uma vez    
    def sendMsg(self, message:str, client:str = ""):
        messageSent = False
        trials      = 5
        waitTime    = 60
        while not messageSent and trials:
            routeKey = self.routeKey
            if len(client) > 0:
                routeKey = f"{self.routeKey}.{client}"

            try:
                connection = pika.BlockingConnection(self.params)
                channel    = connection.channel() # start a channel
                channel.exchange_declare(exchange     = self.exchange, 
                                        exchange_type = 'topic',
                                        auto_delete   = True)
                channel.basic_publish(
                    exchange   = self.exchange, 
                    routing_key= routeKey, 
                    body       = message, 
                    properties = pika.BasicProperties(
                                    content_type = 'text/plain',
                                    delivery_mode= pika.DeliveryMode.Transient))
                messageSent = True
                connection.close()
                del channel, connection 
            except Exception as ex:
                print(f"TROUBLE CNX LOGGER REMAIN ATTEMPT: {trial}")
                time.sleep(waitTime)
                trials -= 1
            

            
class LogService():
    exchange:     Exchange # Servico para envio de logs
    logLocalName: str      # Nome do arquivo de log em disco
    logLocal:     bool     # Grava ou não em /app/logs, volume do host em docker_compose.yml
    Msg:          object   # Objeto representando a mensagem enviada - customizado pelo usuario

    system:  str          # NOME DO SISTEMA (diretorio/docker-compose) E DO EXCHANGE
    service: str          # NOME DO SERVIÇO (docker-compose/services)
    client:  str          # NOME DO CLIENTE NO ATENDIMENTO
    erroMsg: str          # MSG DE ERRO PARA RETORNO CLIENTE
    system:  str          # ID FORNECIDO PELA INSTANCIA
    
    def __str__(self):
        estado = {
            "logLocalName":   self.logLocalName,
            "logLocal":       self.logLocal,
            "system" :        self.system,
            "service":        self.service,
            "client":         self.client, 
            "erroMsg":        self.erroMsg
        }

        return self.pp.pformat(estado)
    
    def __init__(self, system:str, service:str, client:str = "", logType:str = 'logs', logLocal:bool = False):
        self.system        = system.lower()      
        self.service       = service
        self.client        = client
        self.logType       = logType
        self.logLocal      = logLocal
        self.pp            = pprint.PrettyPrinter(indent=4)
        self.logging       = logging
        dt                 = datetime
        self.logLocalName  = 'logs/'+ self.system.split()[0] + '_' + dt.now().strftime("%d")
        self.exchange      = Exchange(system=system, service=service, logType= logType) #logs.hash.sg_etl
        self.erroMsg       = "ERRO"            # MSG DE ERRO PARA RETORNO CLIENTE
        self.logging.basicConfig(filename=self.logLocalName, level=logging.INFO)

    # METODOS INTERNOS _
    # Configura informacoes basicas
    def _setMsg(self, msg:str, caller:str):
        now = datetime.now()
        self.Msg = {
            "system":  self.system,      
            "service": self.service,
            "client":  self.client,      
            "caller":   caller,            
            "logDate":  f"{now}",          
            "msg":      msg                
        }

    # Envia para LogMessage Msg formatada para messageria
    def _sendMsg (self, msg:str, caller: str, *args):
        self._setMsg(msg, caller)
        
        try:
            if self.logLocal:
                self.pp.pprint(self.Msg)
                self.logging.info(self.pp.pformat(self.Msg))
            self.exchange.sendMsg(json.dumps(self.Msg), self.client)
                    
            # outros objetos alem da msg
            for arg in args:
                if len(arg) == 0:
                    break
                msgArg = pprint.pformat(arg)
                self.Msg["msg"] = f"{msgArg}"
                
                if self.logLocal:
                    self.pp.pprint(self.Msg)
                    self.logging.info(self.pp.pformat(self.Msg))
                self.exchange.sendMsg(json.dumps(self.Msg), self.client)
            sys.stdout.flush()
        except Exception as e:
            self.logging.info(self.pp.pformat(e))
            self.pp.pprint(e)
                
    # Envia para LogMessage Msg formatada para messageria
    def _sendMsgE (self, msg:str, caller: str, *args):
        self._setMsg(msg, caller)
        try:
            if self.logLocal:
                self.pp.pprint(self.Msg)
                self.logging.info(self.pp.pformat(self.Msg))
            self.exchange.sendMsg(json.dumps(self.Msg), self.client)
                    
            # outros objetos alem da msg
            for arg in args:
                if len(arg) == 0:
                    break
                msgArg = pprint.pformat(arg)
                self.Msg["msg"] = f"{msgArg}"
                if self.logLocal:
                    self.pp.pprint(self.Msg)
                    self.logging.info(self.pp.pformat(self.Msg))
                self.exchange.sendMsg(json.dumps(self.Msg), self.client)
            sys.stdout.flush()
        except Exception as e:
            self.logging.info(self.pp.pformat(e))
            self.pp.pprint(e)
            sys.stdout.flush()
            
    # METODOS DISPONIBILIZADOS
    ## INFORM  status.<service>.<system>.#
    def heartBeat(self):
        self.exchange.setLogType('status')
        self.logMsg("OK")
        self.exchange.clrLogType()
        
    # Usar quando except ValidationError as e:
    # Atualiza errMsg
    def logMsgValidationError(self, msg: str, e):
        self.exchange.setLogType("error.logs")
        # relaciona os erros encontrados na validacao do modelo
        erros = json.loads(e.json())
        erroLst = []
        for err in erros:
            localErro = '_'.join(err['loc'])
            errMsg    = f"{localErro} => {err['msg']}"
            erroLst.append(errMsg)
            
        self.erroMsg = f"{msg} " + ','.join(erroLst)
        bodyMsg      = f"{self.erroMsg}"
        caller       = inspect.stack()[1].function
        
        self._sendMsgE(bodyMsg, caller)
        self.exchange.clrLogType()

    # Loga e atualiza errMsg
    def logMsgError(self, msg:str, *args):
        self.exchange.setLogType('error.logs')
        self.erroMsg = msg
        caller       = inspect.stack()[1].function
        Msg          = f"{self.erroMsg}"
        self._sendMsgE(Msg, caller, args)
        self.exchange.clrLogType()

    # Log geral
    def logMsg (self, msg: str, *args):
        now = datetime.now()
        caller      = inspect.stack()[1].function
        Msg         = f"{msg}"
        self._sendMsg(Msg, caller, args)
