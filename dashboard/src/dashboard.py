import time, json, sys
import streamlit as st
import extra_streamlit_components as stc
import redis
import datetime
import hashlib

from Exchange import QueueExchange
from Logger import LogService

from functions import *

versao = "1.00 06/12/23  M.J. - Filtro por usuário e trilhas"
versao = "1.01 07/12/23  M.J. - Inicio autenticação: COOKIES"
versao = "1.02 11/12/23  M.J. - autenticação: Security Code"
versao = "1.03 13/12/23  ViniR. - Adicionado SET para não deixar datasets iguais + 2 métricas"
versao = "1.04 14/12/23  ViniR. - Adicionado filtro por data ao Velocidade de Vendas"
versao = "1.05 15/12/23  ViniR. - Criados gráficos e datasets para novas trilhas"
versao = "1.06 15/12/23  M.J. - Adaptação para novo Logger e Exchange"
versao = "1.07 11/01/24  ViniR. - Adicionado filtro de datas para inicio de 2023 a fim de 2024"


# CONFIGURA O LOGGER
if "prev_logger" not in st.session_state:
    try:
        logger = LogService(
            system="sg_intel_vendas", service="dashboard", logType="logs", logLocal=True
        )
        logger.heartBeat()
        logger.logMsg("SERVICE SG_INTEL_VENDAS LOADER STARTING", versao)
        st.session_state["prev_logger"] = logger
    except Exception as e:
        print("SERVICE LOGGER DOWN: ", e)
        time.sleep(10)
        sys.exit(1)
else:
    logger = st.session_state["prev_logger"]

# CONFIGURA O REDIS
# DADOS CONFIGURAÇÃO REDIS
rd = None
if "prev_rd" not in st.session_state:
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
        st.session_state["prev_rd"] = rd
    except Exception as e:
        logger.logMsgError("SERVICE REDIS DOWN: ", e)
        time.sleep(10)
        sys.exit(1)
else:
    rd = st.session_state["prev_rd"]

# CONFIGURE EXCHANGE
if "prev_exchange" not in st.session_state:
    try:
        logger.logMsg("CONNECTING TO EXCHANGE")
        exchange = QueueExchange(service="dashboard")
        st.session_state["prev_exchange"] = exchange
    except Exception as e:
        logger.logMsgError("Erro ao conectar na mensageria: ", e)
        time.sleep(10)
        sys.exit(1)
else:
    exchange = st.session_state["prev_exchange"]

## CONFIGURA A PÁGINA E O ACESSO
st.set_page_config(layout="wide")

# RECEBE E VALIDA O COOKIE
cookie_manager = stc.CookieManager()
cookie = cookie_manager.get(cookie="sistemas.token")

if not cookie:
    logger.logMsgError("COOKIE INEXISTENTE")
    cookies = cookie_manager.get_all()
    st.write("AUTENTICAÇÃO INEXISTENTE")
    logger.logMsg(cookies)
    time.sleep(10)
    sys.exit(1)

if not ':' in cookie or len(cookie.split(':')) != 2:
    logger.logMsgError("FORMATO COOKIE INVALIDO")
    time.sleep(10)
    sys.exit(1)
usr_email, usr_token = cookie.split(":")
# VERIFICA IDENTIFICACAO
usr_authjs = rd.get(f"usr_{usr_email}")
if usr_authjs is None:
    logger.logMsgError(f"AUTENTICACAO INEXISTENTE PARA {usr_email}")
    time.sleep(10)
    sys.exit(1)

try:
    usr_auth = json.loads(usr_authjs)
except:
    st.write(usr_authjs)

if  not (str(usr_token) == str(usr_auth["uuid_original"][:18])):
    logger.logMsgError("AUTENTICACAO INVALIDA NIVEL 1")
    logger.logMsg(f"[{usr_auth['uuid_original'][:18]}] != [{usr_token}]")
    time.sleep(10)
    sys.exit(1)

secretSrc = cookie + usr_auth["uuid_original"] + rd.get("sid").decode("utf-8")
security  = usr_auth["security"]
secret = hashlib.sha256(secretSrc.encode("UTF-8")).hexdigest()
if  not (str(secret) == str(security)):
    logger.logMsgError("AUTENTICACAO INVALIDA NIVEL 2")
    logger.logMsg(f"[{secret}] != [{security}] Cookie [{cookie}]")
    time.sleep(10)
    sys.exit(1)



dadosDashboard = getDados(rd)
if dadosDashboard["status"] == "Inicializando":
    st.title("Inicializando o painel...")
    if "loaderMsg" not in st.session_state:
        st.session_state["loaderMsg"] = True
        time.sleep(10)
        exchange.sendMsg(message="dashboard:start", rkey="loader")
    while True:
        dadosDashboard = getDados(rd)
        time.sleep(5)
        if dadosDashboard["status"] == "Atualizado" or dadosDashboard["status"] == "Atualizando":
            break
        st.rerun()

dadosVGV = getVGV(rd)
st.title("Inteligência de Vendas")

infoGrafico, dataSet = st.tabs(["Gráficos", "Datasets"])

## ======================= ##

## ATUALIZACAO DA TELA
if dadosDashboard['status'] == "":
    st.sidebar.title(f"Última atualização: {dadosDashboard['data']} Vidas: {dadosDashboard['atualizacoes']}")

# CONDIÇÃO PARA TRAVAR A TELA ENQUANTO AGUARDA A ATUALIZAÇÃO DOS DADOS.
if dadosDashboard['status'] == "Atualizando":
    st.sidebar.markdown(
        f"<h2 style='font-size:1em;'>📊 Atualizando aguarde...</h2>",
        unsafe_allow_html=True
    )
    while True:
        dadosDashboard = getDados(rd)
        if dadosDashboard['status'] == "Atualizado":
            st.rerun()
            break
        time.sleep(1)

# CONDIÇÃO PARA CONTINUAR A AMOSTRAGEM DOS DADOS.
if dadosDashboard['status'] == "Atualizado":
    st.sidebar.markdown(
        f"<h2 style='font-size:1em;'>S8 PSA Sistemas</h2>",
        unsafe_allow_html=True
    )
    
    st.sidebar.markdown(
        f"<h2 style='font-size:1em;'>Atualizado!</h2>",
        unsafe_allow_html=True
    )

    st.sidebar.markdown(
        f"<span style='font-size:2em;'>{dadosDashboard['data']}</span>",
        unsafe_allow_html=True
    )

    st.sidebar.markdown(
        f"<span style='font-size:1em;'>Atualizações disponíveis: {dadosDashboard['atualizacoes']}</span>",
        unsafe_allow_html=True
    )

sideCol1, sideCol2 = st.sidebar.columns(2)

emptyCol1, emptyCol2 = st.sidebar.columns(2)

sideCol3, sideCol4 = st.sidebar.columns(2)

emptyCol3, emptyCol4 = st.sidebar.columns(2)

sideCol5, sideCol6 = st.sidebar.columns(2)

emptyCol4, emptyCol5 = st.sidebar.columns(2)

sideCol7, sideCol8 = st.sidebar.columns(2)

emptyCol6, emptyCol7 = st.sidebar.columns(2)

sideCol9, sideCol10 = st.sidebar.columns(2)



## ======================= ##

## ORGANIZAÇÃO PARAMETRIZADA DAS TRILHAS - SIDEBAR
def setTipoGrafico1(nomeTrilha):
    pos = listaNomes.index(nomeTrilha)
    return sideCol2.selectbox("Tipo Gráfico 1", listaTipos[pos])

def setTipoGrafico2(nomeTrilha):
    pos = listaNomes.index(nomeTrilha)
    return sideCol4.selectbox("Tipo Gráfico 2", listaTipos[pos])

def setTipoGrafico3(nomeTrilha):
    pos = listaNomes.index(nomeTrilha)
    return sideCol6.selectbox("Tipo Gráfico 3", listaTipos[pos])

def setTipoGrafico4(nomeTrilha):
    pos = listaNomes.index(nomeTrilha)
    return sideCol8.selectbox("Tipo Gráfico 4", listaTipos[pos])

opcoesTrilhas = rd.get("dashboard_trilhas").decode("utf-8")
opcoesTrilhas = json.loads(opcoesTrilhas)

listaTipos = []
listaNomes = []
userid = 'user_1'
for i in opcoesTrilhas:
    if userid not in i.split(":")[2]:
        continue
    listaNomes.append(i.split(":")[0])
    listaTipos.append(i.split(":")[1].split(","))   


trilhaGrafico1 = sideCol1.selectbox("InfoGráfico 1", listaNomes)
tipoGrafico1  = setTipoGrafico1(trilhaGrafico1)

trilhaGrafico2 = sideCol3.selectbox("InfoGráfico 2", index=1 , options=listaNomes)
tipoGrafico2 = setTipoGrafico2(trilhaGrafico2)

trilhaGrafico3 = sideCol5.selectbox("InfoGráfico 3", listaNomes)
tipoGrafico3  = setTipoGrafico3(trilhaGrafico3)

trilhaGrafico4 = sideCol7.selectbox("InfoGráfico 4", index=2 , options=listaNomes)
tipoGrafico4 = setTipoGrafico4(trilhaGrafico4)

dataInicial = sideCol9.empty()
dataFinal = sideCol10.empty()

valorInicial = dataInicial.date_input("Data Inicial", min_value=datetime.date(2023, 1, 1), max_value=datetime.date(2024, 12, 31), value=datetime.date.today() - datetime.timedelta(days=365))
valorFinal = dataFinal.date_input("Data Final", min_value=datetime.date(2023, 1, 1), max_value=datetime.date(2024, 12, 31), value=datetime.date.today())

st.sidebar.markdown(
    f"<span style='font-size:1em;'>Dados de: {valorInicial.strftime('%m/%Y')} - {valorFinal.strftime('%m/%Y')}</span>",
    unsafe_allow_html=True
)

if st.sidebar.button(f"Atualizar {dadosDashboard['atualizacoes']}/5"):
    dadosDashboard['status'] = "Atualizando"
    dadosDashboardJS = json.dumps(dadosDashboard)
    rd.set("dashboard", dadosDashboardJS)
    exchange.sendMsg("dashboard:start", "loader")
    st.rerun()

## ======================= ##

with infoGrafico:
    ## MÉTRICAS
    VGV1, VGV2, VGV3, VGV4, VGV5 = st.columns(5)
    with VGV1:
        VGV = getVGV(rd)["trilhas_vendas_vgv"]["vgv_bruto"]["value"]
        vgvBruto = f"{VGV:,.2f}".replace(",", "v").replace(".", ",").replace("v", ".")
        st.metric(label="VGV Bruto", value=vgvBruto)

    with VGV2:
        VGV = getVGV(rd)["trilhas_vendas_vgv"]["vgv_cancelado"]["value"]
        vgvCanceladas = f"{VGV:,.2f}".replace(",", "v").replace(".", ",").replace("v", ".")
        st.metric(label="VGV Cancelado", value=vgvCanceladas)

    with VGV3:
        VGV = getVGV(rd)["trilhas_vendas_vgv"]["vgv_liquido"]["value"]
        vgvLiquido = f"{VGV:,.2f}".replace(",", "v").replace(".", ",").replace("v", ".")
        st.metric(label="VGV Líquido", value=vgvLiquido)
        
    with VGV4:
        VGV = getTrilha(rd, "Dashboard-vendas_m2")
        ate25m = VGV["ate25m²"].replace("R$", "").replace(".", "").replace(",", ".")
        ate50m = VGV["ate50m²"].replace("R$", "").replace(".", "").replace(",", ".")
        ate500m = VGV["ate500m²"].replace("R$", "").replace(".", "").replace(",", ".")
        vgvM2 = float(ate25m) + float(ate50m) + float(ate500m)
        st.metric(label="Total Vendas por M²", value=round(vgvM2, 2))
        
    with VGV5:
        VGV = getVGV(rd)["trilhas_vendas_vgv"]["vgv_bruto"]["qty"]
        st.metric(label="Quantidade Contratos", value=VGV)

    ## ======================= ##


    ## COLUNAS DOS GRAFICOS
    col1, col2 = st.columns(2)
    grafico1 = col1.empty()
    grafico2 = col2.empty()

    col3, col4 = st.columns(2)

    grafico3 = col3.empty()
    grafico4 = col4.empty()

    from functions import funcoes

    funcoes[trilhaGrafico1](rd, grafico1, trilhaGrafico1, tipoGrafico1, valorInicial.strftime('%m/%Y'), valorFinal.strftime('%m/%Y'))
    funcoes[trilhaGrafico2](rd, grafico2, trilhaGrafico2, tipoGrafico2, valorInicial.strftime('%m/%Y'), valorFinal.strftime('%m/%Y'))
    funcoes[trilhaGrafico3](rd, grafico3, trilhaGrafico3, tipoGrafico3, valorInicial.strftime('%m/%Y'), valorFinal.strftime('%m/%Y'))
    funcoes[trilhaGrafico4](rd, grafico4, trilhaGrafico4, tipoGrafico4, valorInicial.strftime('%m/%Y'), valorFinal.strftime('%m/%Y'))
    
    ## ======================= ##
    
with dataSet:
    st.title("Datasets")
    
    datasetValidation = set()
    datasetValidation.add(trilhaGrafico1)
    datasetValidation.add(trilhaGrafico2)
    datasetValidation.add(trilhaGrafico3)
    datasetValidation.add(trilhaGrafico4)
    
    for itens in datasetValidation:
        dataset[itens](rd, itens, valorInicial.strftime('%m/%Y'), valorFinal.strftime('%m/%Y'))