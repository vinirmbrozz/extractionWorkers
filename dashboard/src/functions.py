import pandas as pd
import plotly.express as px
import plotly.graph_objs as go
import json
import streamlit as st
# from streamlit_extras.dataframe_explorer import dataframe_explorer
from stealing import dataframe_explorer

versao = "1.0 11/01/24 ViniR. - Mapeada funções de: 'PeriodoQTY, EmpresaQTY, EmpreendQTY junto com seus datasets."

def getDados(rd):
    jsonDashboard = rd.get("dashboard").decode("utf-8")
    jsonDashboard = json.loads(jsonDashboard)
    return jsonDashboard

def getVGV(rd):
    jsonVGV = rd.get("Dashboard-vendas_vgv").decode("utf-8")
    jsonVGV = json.loads(jsonVGV)
    return jsonVGV

def getTrilha(rd,trilha):
    jsonTrilha = rd.get(trilha).decode("utf-8")
    jsonTrilha = json.loads(jsonTrilha)
    return jsonTrilha

def getVendasPeriodo(rd, grafico, titulo, tipo, dataInicial, dataFinal):
    chave = "Dashboard-vendas_vgvperiodo"
    infoGrafico = getTrilha(rd, chave)
    infoGrafico = pd.DataFrame(infoGrafico)
    values = infoGrafico["value"]
    month = infoGrafico["month"]
    year = infoGrafico["year"]
    
    mesInicial = int(dataInicial.split("/")[0])
    anoInicial = int(dataInicial.split("/")[1])
    
    mesFinal = int(dataFinal.split("/")[0])
    anoFinal = int(dataFinal.split("/")[1])

    infoGrafico['date'] = pd.to_datetime(infoGrafico[['year', 'month']].assign(DAY=1))
    filtro = infoGrafico['date'].between(f"{anoInicial}-{mesInicial}", f"{anoFinal}-{mesFinal}")
    
    infoGrafico = infoGrafico[filtro]
    infoGrafico = infoGrafico.drop(columns=['date'])
    
    if tipo == "Linhas":
        figLinha = px.line(
            infoGrafico, x=month, y=values, title=titulo, color=year, labels={'year': 'Ano', 'month': 'Mês', 'value': 'Valor Vendas'}
        )
        grafico.plotly_chart(figLinha, use_container_width=True)
        return 

    cores = px.colors.qualitative.Set1
    figBarras = go.Figure()

    for i, year in enumerate(infoGrafico['year'].unique()):
        dados_ano = infoGrafico[infoGrafico['year'] == year]
        barras_ano = go.Bar(
            x=dados_ano['month'],
            y=dados_ano['value'],
            name=str(year),
            marker_color=cores[i]  # Atribui cores diferentes para cada ano
        )
        figBarras.add_trace(barras_ano)

    figBarras.update_layout(
        xaxis=dict(title='Mês'),
        yaxis=dict(title='Valor Vendas'),
        title=titulo,
        barmode='group'  # Agrupa as barras lado a lado
    )
    grafico.plotly_chart(figBarras, use_container_width=True)
    
def getVendasPeriodoDataSet(rd, titulo, dataInicial, dataFinal):
    chave = "Dashboard-vendas_vgvperiodo"
    infoGrafico = getTrilha(rd, chave)
    infoGrafico = pd.DataFrame(infoGrafico)
    
    st.markdown(
        f"<h2 style='font-size:1em;'>{titulo} - de: {dataInicial} até: {dataFinal}</h2>",
        unsafe_allow_html=True
    )
    
    mesInicial = int(dataInicial.split("/")[0])
    anoInicial = int(dataInicial.split("/")[1])
    
    mesFinal = int(dataFinal.split("/")[0])
    anoFinal = int(dataFinal.split("/")[1])

    infoGrafico['date'] = pd.to_datetime(infoGrafico[['year', 'month']].assign(DAY=1))
    filtro = infoGrafico['date'].between(f"{anoInicial}-{mesInicial}", f"{anoFinal}-{mesFinal}")
    
    infoGrafico = infoGrafico[filtro]
    infoGrafico = infoGrafico.drop(columns=['date'])
    
    infoGrafico.rename(columns={'value': 'Valor Vendas', 'year': 'Ano', 'month': 'Mês'}, inplace=True)
    filtroDf = dataframe_explorer(infoGrafico)
    st.dataframe(filtroDf, width=800, height=400)
    st.markdown("---")
    
def getVendasEmpresa(rd, grafico, titulo, tipo, dataInicial, dataFinal):
    monthOrder = [1,2,3,4,5,6,7,8,9,10,11,12]
    chave = "Dataset-vendas_vgvempresa"
    infoGrafico = getTrilha(rd, chave)
    infoGrafico = pd.DataFrame(infoGrafico)
    infoGrafico['month'] = pd.Categorical(infoGrafico['month'], categories=monthOrder, ordered=True)
    infoGrafico = infoGrafico.sort_values(by=['year', 'month'])
    
    mesInicial = int(dataInicial.split("/")[0])
    anoInicial = int(dataInicial.split("/")[1])
    
    mesFinal = int(dataFinal.split("/")[0])
    anoFinal = int(dataFinal.split("/")[1])
    
    infoGrafico['date'] = pd.to_datetime(infoGrafico[['year', 'month']].assign(DAY=1))
    filtro = infoGrafico['date'].between(f"{anoInicial}-{mesInicial}", f"{anoFinal}-{mesFinal}")
    
    infoGrafico = infoGrafico[filtro]
    infoGrafico = infoGrafico.drop(columns=['date'])

    infoGrafico = infoGrafico.groupby(['companyName'])['value'].sum().reset_index()
    
    infoGrafico['firstName'] = infoGrafico['companyName'].str[:16]
    
    if tipo == "Linhas":
        figLinha = px.line(
            infoGrafico, x='firstName', y='value', title=titulo, labels={'firstName': 'Empresas', 'value': 'Valor Vendas'}
        )
        grafico.plotly_chart(figLinha, use_container_width=True)
        return
    
    figBarras = go.Figure()
    
    
    figBarras = px.bar(
        infoGrafico,
        x=infoGrafico['firstName'],
        y=infoGrafico['value'],
        color=infoGrafico['firstName'],
        title=titulo,
        barmode='group',
        labels={'firstName': 'Empresas', 'value': 'Valor Vendas'}
    )
    
    grafico.plotly_chart(figBarras, use_container_width=True)
    
def getVendasEmpresaDataSet(rd, titulo, dataInicial, dataFinal):
    monthOrder = [1,2,3,4,5,6,7,8,9,10,11,12]
    chave = "Dataset-vendas_vgvempresa"
    infoGrafico = getTrilha(rd, chave)
    infoGrafico = pd.DataFrame(infoGrafico)
    infoGrafico['month'] = pd.Categorical(infoGrafico['month'], categories=monthOrder, ordered=True)
    infoGrafico = infoGrafico.sort_values(by=['year', 'month'])
    
    mesInicial = int(dataInicial.split("/")[0])
    anoInicial = int(dataInicial.split("/")[1])
    
    mesFinal = int(dataFinal.split("/")[0])
    anoFinal = int(dataFinal.split("/")[1])
    
    infoGrafico['date'] = pd.to_datetime(infoGrafico[['year', 'month']].assign(DAY=1))
    filtro = infoGrafico['date'].between(f"{anoInicial}-{mesInicial}", f"{anoFinal}-{mesFinal}")
    
    infoGrafico = infoGrafico[filtro]
    infoGrafico = infoGrafico.drop(columns=['date'])
    
    st.markdown(
        f"<h2 style='font-size:1em;'>{titulo} - de: {dataInicial} até: {dataFinal}</h2>",
        unsafe_allow_html=True
    )
    infoGrafico.rename(columns={'value': 'Valor Vendas', 'companyName': 'Empresa', 'year': 'Ano', 'month': 'Mês', 'value_acumulado': 'Valor Acumulado'}, inplace=True)
    
    filtroDf = dataframe_explorer(infoGrafico)
    st.dataframe(filtroDf, width=800, height=400)
    st.markdown("---")
    
def getVelocidadeVendas(rd, grafico, titulo, tipo, dataInicial, dataFinal):
    monthValues = {'JAN': 1,
                   'FEV': 2,
                   'MAR': 3,
                   'ABR': 4,
                   'MAI': 5,
                   'JUN': 6,
                   'JUL': 7,
                   'AGO': 8,
                   'SET': 9,
                   'OUT': 10,
                   'NOV': 11,
                   'DEZ': 12
                   }
    yearValues = {
        '22': 2022,
        '23': 2023
    }
    chave = "Dashboard-vendas_velocidadevendas"
    infoGrafico = getTrilha(rd, chave)
    infoGrafico = pd.DataFrame(infoGrafico)
    
    mesInicial = int(dataInicial.split("/")[0])
    anoInicial = int(dataInicial.split("/")[1])
    
    mesFinal = int(dataFinal.split("/")[0])
    anoFinal = int(dataFinal.split("/")[1])
    
    infoGrafico['month'] = infoGrafico['data'].str[:3].map(monthValues)
    infoGrafico['year'] = infoGrafico['data'].str[-2:].map(yearValues)
    infoGrafico = infoGrafico.sort_values(by=['year', 'month'])
    infoGrafico['valor'] = infoGrafico['valor'] * 100
    
    infoGrafico['date'] = pd.to_datetime(infoGrafico[['year', 'month']].assign(DAY=1))
    filtro = infoGrafico['date'].between(f"{anoInicial}-{mesInicial}", f"{anoFinal}-{mesFinal}")
    
    infoGrafico = infoGrafico[filtro]
    infoGrafico = infoGrafico.drop(columns=['date'])
    
    if tipo == "Linhas":
        figLinha = px.line(
            infoGrafico, x='data', y='valor', color='nome', title=titulo, labels={'data': 'Data', 'valor': 'Valor %', 'nome': 'Nome'}
        )
        grafico.plotly_chart(figLinha, use_container_width=True)
        return
    
    figBarras = px.bar(
        infoGrafico,
        x=infoGrafico['data'],
        y=infoGrafico['valor'],
        color=infoGrafico['nome'],
        title=titulo,
        barmode='group',
        labels={'data': 'Data', 'valor': 'Valor %', 'nome': 'Nome'}
    )
    grafico.plotly_chart(figBarras, use_container_width=True)
    
def getVelocidadeVendasDataSet(rd, titulo, dataInicial, dataFinal):    
    monthValues = {'JAN': 1,
                   'FEV': 2,
                   'MAR': 3,
                   'ABR': 4,
                   'MAI': 5,
                   'JUN': 6,
                   'JUL': 7,
                   'AGO': 8,
                   'SET': 9,
                   'OUT': 10,
                   'NOV': 11,
                   'DEZ': 12
                   }
    yearValues = {
        '22': 2022,
        '23': 2023
    }
    
    chave = "Dashboard-vendas_velocidadevendas"
    infoGrafico = getTrilha(rd, chave)
    infoGrafico = pd.DataFrame(infoGrafico)
    
    mesInicial = int(dataInicial.split("/")[0])
    anoInicial = int(dataInicial.split("/")[1])
    
    mesFinal = int(dataFinal.split("/")[0])
    anoFinal = int(dataFinal.split("/")[1])
    
    infoGrafico['month'] = infoGrafico['data'].str[:3].map(monthValues)
    infoGrafico['year'] = infoGrafico['data'].str[-2:].map(yearValues)
    infoGrafico = infoGrafico.sort_values(by=['year', 'month'])
    infoGrafico['valor'] = infoGrafico['valor'] * 100
    
    infoGrafico['date'] = pd.to_datetime(infoGrafico[['year', 'month']].assign(DAY=1))
    filtro = infoGrafico['date'].between(f"{anoInicial}-{mesInicial}", f"{anoFinal}-{mesFinal}")
    
    infoGrafico = infoGrafico[filtro]
    infoGrafico = infoGrafico.drop(columns=['date'])
     
    infoGrafico.rename(columns={'valor': 'Valor %', 'month': 'Mês', 'nome': 'Nome', 'data': 'Data', 'year': 'Ano'}, inplace=True)
    
    st.markdown(
        f"<h2 style='font-size:1em;'>{titulo} - de: {dataInicial} até: {dataFinal}</h2>",
        unsafe_allow_html=True
    )
    
    filtroDf = dataframe_explorer(infoGrafico)
    st.dataframe(filtroDf, width=800, height=400)
    st.markdown("---")
    
def getEstoqueEmpreendimento(rd, grafico, titulo, tipo, dataInicial, dataFinal):
    # Pré-processamento para criar uma lista de dicionários para 'tipos'
    chave= "Dashboard-vendas_units_estoque_empreendimento"
    dados = getTrilha(rd, chave)
    novos_dados = []
    for d in dados:
        novo_d = d.copy()
        novo_d['tipos'] = [dict(tipo=tipo, quantidade=quantidade) for tipo, quantidade in d['tipos'].items()]
        novos_dados.append(novo_d)

    # Convertendo dados para DataFrame
    df = pd.json_normalize(novos_dados, 'tipos', ['enterpriseId', 'name'])

    # Criando gráfico de barras com Plotly Express
    fig = px.bar(
         df,
    x='tipo',
    y='quantidade',
    color='tipo',
    facet_col='name',
    labels={"quantidade": "Quantidade", "tipo": "Tipo"},
    title="Estoque por Empreendimento"
    )

    # Exibindo o gráfico no app Streamlit
    grafico.plotly_chart(fig, use_container_width=True)

def getEstoqueEmpreendimentoDataSet(rd, titulo, dataInicial, dataFinal):
    chave= "Dashboard-vendas_units_estoque_empreendimento"
    infoGrafico = getTrilha(rd, chave)
    
    infoGrafico = pd.json_normalize(infoGrafico)
    infoGrafico.columns = [col.replace("tipos.", "") for col in infoGrafico.columns]

    st.markdown(
        f"<h2 style='font-size:1em;'>{titulo}</h2>",
        unsafe_allow_html=True
    )
    
    infoGrafico.rename(columns={'mes':'Mês', 'ano':'Ano', 'name': 'Empreendimento', 'enterpriseId': 'N° Empreendimento', 'companyId': 'N° Empresa', 'total': 'Total', 'property_type': 'Tipo Propriedade', 'deliveryDate': 'Data Entrega'}, inplace=True)
    
    filtroDf = dataframe_explorer(infoGrafico)
    st.dataframe(filtroDf, width=800, height=400)
    st.markdown("---")

def getVendasCorretor(rd, grafico, titulo, tipo, dataInicial, dataFinal):

    chave_redis = "Dashboard-vendas_corretor"  
    infoCorretores = getTrilha(rd,chave_redis)
    infoCorretores = pd.DataFrame(infoCorretores)
    
    anoInicial = int(dataInicial.split("/")[1])
    mesInicial = int(dataInicial.split("/")[0])
    anoFinal = int(dataFinal.split("/")[1])
    mesFinal = int(dataFinal.split("/")[0])
    infoCorretores['date'] = pd.to_datetime(infoCorretores[['year', 'month']].assign(DAY=1))
    filtro = infoCorretores['date'].between(f"{anoInicial}-{mesInicial}", f"{anoFinal}-{mesFinal}")
    
    infoCorretores = infoCorretores[filtro]
    infoCorretores = infoCorretores.drop(columns=['date'])
    
    infoCorretores =  infoCorretores.groupby(["name"])['vgv'].sum().reset_index()

    if tipo == "Pizza":
        figPizza = px.pie(
        infoCorretores,
        values="vgv",
        names="name",
        title=titulo
        )
        grafico.plotly_chart(figPizza, use_container_width=True)
        return

    # cores = px.colors.qualitative.Set1
    figBarras = go.Figure()

    figBarras = px.bar(infoCorretores, x="name", y="vgv", color="name", title=titulo, labels={'name': 'Corretor', 'vgv': 'Valor Vendas'})

    figBarras.update_layout(
    xaxis=dict(title='Corretor'),
    yaxis=dict(title='Valor Vendas')
    )


    grafico.plotly_chart(figBarras, use_container_width=True)

def getVendasCorretorDataset(rd, titulo, dataInicial, dataFinal):
    chave_redis = "Dashboard-vendas_corretor"  
    infoCorretores = getTrilha(rd,chave_redis)
    infoCorretores = pd.DataFrame(infoCorretores)
    
    anoInicial = int(dataInicial.split("/")[1])
    mesInicial = int(dataInicial.split("/")[0])
    anoFinal = int(dataFinal.split("/")[1])
    mesFinal = int(dataFinal.split("/")[0])
    infoCorretores['date'] = pd.to_datetime(infoCorretores[['year', 'month']].assign(DAY=1))
    filtro = infoCorretores['date'].between(f"{anoInicial}-{mesInicial}", f"{anoFinal}-{mesFinal}")
    
    infoCorretores = infoCorretores[filtro]
    infoCorretores = infoCorretores.drop(columns=['date'])
    
    infoCorretores.rename(columns={'name': 'Nome', 'month': 'Mês', 'year': 'Ano', 'vgv': 'Valor Vendas'}, inplace=True)

    st.markdown(
        f"<h2 style='font-size:1em;'>{titulo} - de: {dataInicial} até: {dataFinal}</h2>",
        unsafe_allow_html=True
    )
    
    filtroDf = dataframe_explorer(infoCorretores)
    st.dataframe(filtroDf, width=800, height=400)
    st.markdown("---")
       
def getVendasIdade(rd, grafico, titulo, tipo, dataInicial, dataFinal):

    chave_redis = "Dashboard-vendas_idade"  
    infoIdade = getTrilha(rd,chave_redis)
    infoIdade = pd.DataFrame(infoIdade)
    
    anoicial = int(dataInicial.split("/")[1])
    mesicial = int(dataInicial.split("/")[0])
    anoFinal = int(dataFinal.split("/")[1])
    mesFinal = int(dataFinal.split("/")[0])
    infoIdade['date'] = pd.to_datetime(infoIdade[['year', 'month']].assign(DAY=1))
    filtro = infoIdade['date'].between(f"{anoicial}-{mesicial}", f"{anoFinal}-{mesFinal}")
    
    infoIdade = infoIdade[filtro]
    infoIdade = infoIdade.drop(columns=['date'])
    
    def age_range(age):
        if 18 <= age <= 30:
            return '18-30'
        elif 31 <= age <= 50:
            return '31-50'
        elif age > 50:
            return '50+'

    infoIdade['age_range'] = infoIdade['value'].apply(age_range)
    infoIdade = infoIdade.groupby('age_range')['vgv'].sum().reset_index()

    if tipo == "Pizza":
        figPizza = px.pie(
        infoIdade,
        values="vgv",
        names="age_range",
        title=titulo
        )
        grafico.plotly_chart(figPizza, use_container_width=True)

    if tipo == "Barras":
        figBarras = px.bar(
        infoIdade,
        x="age_range",
        y="vgv",
        color="age_range",
        title=titulo
        )
    
        figBarras.update_layout(
        xaxis=dict(title='Idade'),
        yaxis=dict(title='Valor Vendas')
        )
        grafico.plotly_chart(figBarras, use_container_width=True)

def getVendasIdadeDataset(rd, titulo, dataInicial, dataFinal):
    chave_redis = "Dashboard-vendas_idade"  
    infoIdade = getTrilha(rd,chave_redis)
    infoIdade = pd.DataFrame(infoIdade)
    
    anoInicial = int(dataInicial.split("/")[1])
    mesInicial = int(dataInicial.split("/")[0])
    anoFinal = int(dataFinal.split("/")[1])
    mesFinal = int(dataFinal.split("/")[0])
    infoIdade['date'] = pd.to_datetime(infoIdade[['year', 'month']].assign(DAY=1))
    filtro = infoIdade['date'].between(f"{anoInicial}-{mesInicial}", f"{anoFinal}-{mesFinal}")
    
    infoIdade = infoIdade[filtro]
    infoIdade = infoIdade.drop(columns=['date'])
    infoIdade = infoIdade.groupby(['value'])['vgv'].sum().reset_index()
    infoIdade.rename(columns={'value': 'Idade', 'vgv': 'Valor Vendas'}, inplace=True)

    st.markdown(
        f"<h2 style='font-size:1em;'>{titulo} - de: {dataInicial} até: {dataFinal}</h2>",
        unsafe_allow_html=True
    )

    filtroDf = dataframe_explorer(infoIdade)
    st.dataframe(filtroDf, width=800, height=400)
    st.markdown("---")

def getVendasClientesCidade(rd, grafico, titulo, tipo, dataInicial, dataFinal):
    chave= "Dashboard-vendas_customers_cidade"
    dados = getTrilha(rd, chave)
    infoClientes = pd.DataFrame(dados)
    anoInicial = int(dataInicial.split("/")[1])
    mesInicial = int(dataInicial.split("/")[0])
    anoFinal = int(dataFinal.split("/")[1])
    mesFinal = int(dataFinal.split("/")[0])
    
    infoClientes['date'] = pd.to_datetime(infoClientes[['year', 'month']].assign(DAY=1))
    filtro = infoClientes['date'].between(f"{anoInicial}-{mesInicial}", f"{anoFinal}-{mesFinal}")
    
    infoClientes = infoClientes[filtro]
    infoClientes = infoClientes.drop(columns=['date'])

    if tipo == "Barras":
        figPizza = px.pie(
        infoClientes,
        values="value",
        names="name",
        title="Vendas por cliente por cidade",
        )
        grafico.plotly_chart(figPizza, use_container_width=True)
        
        figBarras = px.bar(infoClientes, x="name", y="value", color="name", title="Vendas por cidade cliente")
        figBarras.update_layout(
            xaxis=dict(title='Corretor'),
            yaxis=dict(title='Valor Vendas')
            )


        grafico.plotly_chart(figBarras, use_container_width=True)

    if tipo == "Pizza":
        figPizza = px.pie(
        infoClientes,
        values="value",
        names="name",
        title="Vendas por cliente por cidade",
        )
        grafico.plotly_chart(figPizza, use_container_width=True)

def getVendasClientesCidadeDataset(rd, titulo, dataInicial, dataFinal):
    chave_redis = "Dashboard-vendas_customers_cidade"  
    infoCidades = getTrilha(rd,chave_redis)
    infoCidades = pd.DataFrame(infoCidades)
    
    anoicial = int(dataInicial.split("/")[1])
    mesicial = int(dataInicial.split("/")[0])
    anoFinal = int(dataFinal.split("/")[1])
    mesFinal = int(dataFinal.split("/")[0])
    infoCidades['date'] = pd.to_datetime(infoCidades[['year', 'month']].assign(DAY=1))
    filtro = infoCidades['date'].between(f"{anoicial}-{mesicial}", f"{anoFinal}-{mesFinal}")
    
    infoCidades = infoCidades[filtro]
    infoCidades = infoCidades.drop(columns=['date'])
    infoCidades = infoCidades.groupby(['name','year','month'])['value'].sum().reset_index()
    infoCidades.rename(columns={'name': 'Cidade', 'value': 'Valor Vendas','year': 'Ano', 'month': 'Mês'}, inplace=True)
    st.markdown(
        f"<h2 style='font-size:1em;'>{titulo} - de: {dataInicial} até: {dataFinal}</h2>",
        unsafe_allow_html=True
    )

    filtroDf = dataframe_explorer(infoCidades)
    st.dataframe(filtroDf, width=800, height=400)
    st.markdown("---")

def getVendasClientesUF(rd, grafico, titulo, tipo, dataInicial, dataFinal):
    chave = "Dashboard-vendas_customers_uf"
    dados = getTrilha(rd, chave)
    infoClientes = pd.DataFrame(dados)
    anoInicial = int(dataInicial.split("/")[1])
    mesInicial = int(dataInicial.split("/")[0])
    anoFinal = int(dataFinal.split("/")[1])
    mesFinal = int(dataFinal.split("/")[0])

    infoClientes['date'] = pd.to_datetime(infoClientes[['year', 'month']].assign(DAY=1))
    filtro = infoClientes['date'].between(f"{anoInicial}-{mesInicial}", f"{anoFinal}-{mesFinal}")

    infoClientes = infoClientes[filtro]
    infoClientes = infoClientes.drop(columns=['date'])

    if tipo == "Barras":
        figPizza = px.pie(
        infoClientes,
        values="value",
        names="name",
        title="Vendas por cliente por UF",
        )
        grafico.plotly_chart(figPizza, use_container_width=True)

        figBarras = px.bar(infoClientes, x="name", y="value", color="name", title="Vendas por UF")
        figBarras.update_layout(
            xaxis=dict(title='UF'),
            yaxis=dict(title='Valor Vendas')
            )

        grafico.plotly_chart(figBarras, use_container_width=True)

    if tipo == "Pizza":
        figPizza = px.pie(
        infoClientes,
        values="value",
        names="name",
        title="Vendas por cliente por UF",
        )
        grafico.plotly_chart(figPizza, use_container_width=True)

def getVendasClientesUFDataset(rd, titulo, dataInicial, dataFinal):
    chave = "Dashboard-vendas_customers_uf"
    infoCidades = getTrilha(rd, chave)
    infoCidades = pd.DataFrame(infoCidades)

    anoInicial = int(dataInicial.split("/")[1])
    mesInicial = int(dataInicial.split("/")[0])
    anoFinal = int(dataFinal.split("/")[1])
    mesFinal = int(dataFinal.split("/")[0])

    infoCidades['date'] = pd.to_datetime(infoCidades[['year', 'month']].assign(DAY=1))
    filtro = infoCidades['date'].between(f"{anoInicial}-{mesInicial}", f"{anoFinal}-{mesFinal}")

    infoCidades = infoCidades[filtro]
    infoCidades = infoCidades.drop(columns=['date'])
    infoCidades = infoCidades.groupby(['name','year','month'])['value'].sum().reset_index()
    infoCidades.rename(columns={'name': 'UF', 'value': 'Valor Vendas','year': 'Ano', 'month': 'Mês'}, inplace=True)
    st.markdown(
        f"<h2 style='font-size:1em;'>{titulo} - de: {dataInicial} até: {dataFinal}</h2>",
        unsafe_allow_html=True
    )

    filtroDf = dataframe_explorer(infoCidades)
    st.dataframe(filtroDf, width=800, height=400)
    st.markdown("---")
    
def getVendasM2Estq(rd, grafico, titulo, tipo, dataInicial, dataFinal):
    chave= "Dashboard-vendas_m2_estq"
    dados = getTrilha(rd, chave)
    valores_numericos = []
    for valor in dados.values():
        try:
            # Remover "R$ ", substituir "," por "" e converter para float
            valor_numerico = float(valor.replace("R$ ", "").replace(",", ""))
            valores_numericos.append(valor_numerico)
        except ValueError as e:
            # Tratar o erro (pode imprimir uma mensagem ou tomar outra ação)
            print(f"Erro ao converter valor: {e}")

    # Criando gráfico de barras
    if tipo == "Barras":
        fig = px.bar(
            x=list(dados.keys()),
            y=valores_numericos,
            labels={"x": "Categorias", "y": "Valores"},
            title=titulo,
        )
    if tipo == "Pizza":
        fig = px.pie(
            names=list(dados.keys()),
            values=valores_numericos,
            title=titulo,
        )

    # Exibindo o gráfico de barras
    grafico.plotly_chart(fig, use_container_width=True)

def getVendasM2EstqDataset(rd, titulo, dataInicial, dataFinal):
    chave= "Dashboard-vendas_m2_estq"
    infoGrafico = getTrilha(rd, chave)
    infoGrafico = pd.json_normalize(infoGrafico)
    infoGrafico.rename(columns={'ate25m²': '25m²', 'ate50m²': '50m²', 'ate500m²': '500m²'})

    st.markdown(
        f"<h2 style='font-size:1em;'>{titulo}</h2>",
        unsafe_allow_html=True
    )

    filtroDf = dataframe_explorer(infoGrafico)
    st.dataframe(filtroDf, width=800, height=400)
    st.markdown("---")

def getVendasM2(rd, grafico, titulo, tipo, dataInicial, dataFinal):
    chave= "Dashboard-vendas_m2"
    dados = getTrilha(rd, chave)
    valores_numericos = []
    for valor in dados.values():
        try:
            # Remover "R$ ", substituir "," por "" e converter para float
            valor_numerico = float(valor.replace("R$ ", "").replace(",", ""))
            valores_numericos.append(valor_numerico)
        except ValueError as e:
            # Tratar o erro (pode imprimir uma mensagem ou tomar outra ação)
            print(f"Erro ao converter valor: {e}")

    # Criando gráfico de barras
    if tipo == "Barras":
        fig = px.bar(
            x=list(dados.keys()),
            y=valores_numericos,
            labels={"x": "Categorias", "y": "Valores"},
            title=titulo,
        )
    if tipo == "Pizza":
        fig = px.pie(
            names=list(dados.keys()),
            values=valores_numericos,
            title=titulo,
        )

    # Exibindo o gráfico de barras
    grafico.plotly_chart(fig, use_container_width=True)

def getVendasM2Dataset(rd, titulo, dataInicial, dataFinal):
    chave= "Dashboard-vendas_m2"
    infoGrafico = getTrilha(rd, chave)
    infoGrafico = pd.json_normalize(infoGrafico)
    infoGrafico.rename(columns={'ate25m²': '25m²', 'ate50m²': '50m²', 'ate500m²': '500m²'})

    st.markdown(
        f"<h2 style='font-size:1em;'>{titulo}</h2>",
        unsafe_allow_html=True
    )

    filtroDf = dataframe_explorer(infoGrafico)
    st.dataframe(filtroDf, width=800, height=400)
    st.markdown("---")

def getVendasEmpreedimentosCidades(rd, grafico, titulo, tipo, dataInicial, dataFinal):
    chave= "Dashboard-vendas_enterprises_cidade"
    dados = getTrilha(rd, chave)
    infoCidades = pd.DataFrame(dados)

    anoInicial = int(dataInicial.split("/")[1])
    mesInicial = int(dataInicial.split("/")[0])
    anoFinal = int(dataFinal.split("/")[1])
    mesFinal = int(dataFinal.split("/")[0])

    infoCidades['date'] = pd.to_datetime(infoCidades[['year', 'month']].assign(DAY=1))
    filtro = infoCidades['date'].between(f"{anoInicial}-{mesInicial}", f"{anoFinal}-{mesFinal}")

    infoCidades = infoCidades[filtro]
    infoCidades = infoCidades.drop(columns=['date'])
    infoCidades = infoCidades.groupby(['name','year','month'])['value'].sum().reset_index()

    if tipo == "Barras":
        figBarras = px.bar(infoCidades, x="name", y="value", color="name", title="Vendas por cidade")

        figBarras.update_layout(
            xaxis=dict(title='Cidade'),
            yaxis=dict(title='Valor Vendas')
            )


        grafico.plotly_chart(figBarras, use_container_width=True)

    if tipo == "Pizza":
        figPizza = px.pie(
        infoCidades,
        values="value",
        names="name",
        title="Vendas por Cidade",
        )
        grafico.plotly_chart(figPizza, use_container_width=True) 

def getVendasEmpreedimentosCidadesDataset(rd, titulo, dataInicial, dataFinal):
    chave= "Dashboard-vendas_enterprises_cidade"
    dados = getTrilha(rd, chave)
    infoCidades = pd.DataFrame(dados)

    anoInicial = int(dataInicial.split("/")[1])
    mesInicial = int(dataInicial.split("/")[0])
    anoFinal = int(dataFinal.split("/")[1])
    mesFinal = int(dataFinal.split("/")[0])

    infoCidades['date'] = pd.to_datetime(infoCidades[['year', 'month']].assign(DAY=1))
    filtro = infoCidades['date'].between(f"{anoInicial}-{mesInicial}", f"{anoFinal}-{mesFinal}")

    infoCidades = infoCidades[filtro]
    infoCidades = infoCidades.drop(columns=['date'])
    infoCidades = infoCidades.groupby(['name','year','month'])['value'].sum().reset_index()
    infoCidades.rename(columns={'name': 'Cidade', 'value': 'Valor Vendas','year': 'Ano', 'month': 'Mês'}, inplace=True)

    st.markdown(
        f"<h2 style='font-size:1em;'>{titulo} - de: {dataInicial} até: {dataFinal}</h2>",
        unsafe_allow_html=True
    )

    filtroDf = dataframe_explorer(infoCidades)
    st.dataframe(filtroDf, width=800, height=400)
    st.markdown("---")

def getVendasEmpreedimentosUf(rd, grafico, titulo, tipo, dataInicial, dataFinal):
    chave= "Dashboard-vendas_enterprises_uf"
    dados = getTrilha(rd, chave)
    infoCidades = pd.DataFrame(dados)

    anoInicial = int(dataInicial.split("/")[1])
    mesInicial = int(dataInicial.split("/")[0])
    anoFinal = int(dataFinal.split("/")[1])
    mesFinal = int(dataFinal.split("/")[0])

    infoCidades['date'] = pd.to_datetime(infoCidades[['year', 'month']].assign(DAY=1))
    filtro = infoCidades['date'].between(f"{anoInicial}-{mesInicial}", f"{anoFinal}-{mesFinal}")

    infoCidades = infoCidades[filtro]
    infoCidades = infoCidades.drop(columns=['date'])
    infoCidades = infoCidades.groupby(['name','year','month'])['value'].sum().reset_index()

    if tipo == "Barras":
        figBarras = px.bar(infoCidades, x="name", y="value", color="name", title="Vendas por cidade")

        figBarras.update_layout(
            xaxis=dict(title='UF'),
            yaxis=dict(title='Valor Vendas')
            )


        grafico.plotly_chart(figBarras, use_container_width=True)

    if tipo == "Pizza":
        figPizza = px.pie(
        infoCidades,
        values="value",
        names="name",
        title="Vendas por UF",
        )
        grafico.plotly_chart(figPizza, use_container_width=True) 

def getVendasEmpreedimentosUfDataset(rd, titulo, dataInicial, dataFinal):
    chave= "Dashboard-vendas_enterprises_uf"
    dados = getTrilha(rd, chave)
    infoCidades = pd.DataFrame(dados)

    anoInicial = int(dataInicial.split("/")[1])
    mesInicial = int(dataInicial.split("/")[0])
    anoFinal = int(dataFinal.split("/")[1])
    mesFinal = int(dataFinal.split("/")[0])

    infoCidades['date'] = pd.to_datetime(infoCidades[['year', 'month']].assign(DAY=1))
    filtro = infoCidades['date'].between(f"{anoInicial}-{mesInicial}", f"{anoFinal}-{mesFinal}")

    infoCidades = infoCidades[filtro]
    infoCidades = infoCidades.drop(columns=['date'])
    infoCidades = infoCidades.groupby(['name','year','month'])['value'].sum().reset_index()
    infoCidades.rename(columns={'name': 'UF', 'value': 'Valor Vendas','year': 'Ano', 'month': 'Mês'}, inplace=True)

    st.markdown(
        f"<h2 style='font-size:1em;'>{titulo} - de: {dataInicial} até: {dataFinal}</h2>",
        unsafe_allow_html=True
    )

    filtroDf = dataframe_explorer(infoCidades)
    st.dataframe(filtroDf, width=800, height=400)
    st.markdown("---")

def getVendasSexo(rd, grafico, titulo, tipo, dataInicial, dataFinal):
    chave = "Dashboard-vendas_sexo"
    dados = getTrilha(rd, chave)
    infoSexo = pd.DataFrame(dados)

    anoInicial = int(dataInicial.split("/")[1])
    anoInicial = int(dataInicial.split("/")[0])
    anoFinal = int(dataFinal.split("/")[1])
    mesFinal = int(dataFinal.split("/")[0])

    infoSexo['date'] = pd.to_datetime(infoSexo[['year', 'month']].assign(DAY=1))
    filtro = infoSexo['date'].between(f"{anoInicial}-{anoInicial}", f"{anoFinal}-{mesFinal}")

    infoSexo = infoSexo[filtro]
    infoSexo = infoSexo.drop(columns=['date'])
    infoSexo = infoSexo.groupby(['name','year','month'])['value'].sum().reset_index()

    if tipo == "Barras":
        figBarras = px.bar(infoSexo, x="name", y="value", color="name", title="Vendas por sexo")

        figBarras.update_layout(
            xaxis=dict(title='Sexo'),
            yaxis=dict(title='Valor Vendas')
            )


        grafico.plotly_chart(figBarras, use_container_width=True)
    
    if tipo == "Pizza":
        figPizza = px.pie(
        infoSexo,
        values="value",
        names="name",
        title="Vendas por sexo",
        )
        grafico.plotly_chart(figPizza, use_container_width=True)

def getVendasSexoDataset(rd, titulo, dataInicial, dataFinal):
    chave = "Dashboard-vendas_sexo"
    dados = getTrilha(rd, chave)
    infoSexo = pd.DataFrame(dados)

    anoInicial = int(dataInicial.split("/")[1])
    mesInicial = int(dataInicial.split("/")[0])
    anoFinal = int(dataFinal.split("/")[1])
    mesFinal = int(dataFinal.split("/")[0])

    infoSexo['date'] = pd.to_datetime(infoSexo[['year', 'month']].assign(DAY=1))
    filtro = infoSexo['date'].between(f"{anoInicial}-{mesInicial}", f"{anoFinal}-{mesFinal}")

    infoSexo = infoSexo[filtro]
    infoSexo = infoSexo.drop(columns=['date'])
    infoSexo = infoSexo.groupby(['name','year','month'])['value'].sum().reset_index()
    infoSexo.rename(columns={'name': 'Sexo', 'value': 'Valor Vendas','year': 'Ano', 'month': 'Mês'}, inplace=True)

    st.markdown(
        f"<h2 style='font-size:1em;'>{titulo} - de: {dataInicial} até: {dataFinal}</h2>",
        unsafe_allow_html=True
    )
    st.dataframe(infoSexo, width=800, height=400)
    st.markdown("---")

def getVendasProfissoes(rd, grafico, titulo, tipo, dataInicial, dataFinal):
    chave = "Dashboard-vendas_profissoes"
    dados = getTrilha(rd, chave)
    infoProfissoes = pd.DataFrame(dados)

    anoInicial = int(dataInicial.split("/")[1])
    mesInicial = int(dataInicial.split("/")[0])
    anoFinal = int(dataFinal.split("/")[1])
    mesFinal = int(dataFinal.split("/")[0])

    infoProfissoes['date'] = pd.to_datetime(infoProfissoes[['year', 'month']].assign(DAY=1))
    filtro = infoProfissoes['date'].between(f"{anoInicial}-{mesInicial}", f"{anoFinal}-{mesFinal}")

    infoProfissoes = infoProfissoes[filtro]
    infoProfissoes = infoProfissoes.drop(columns=['date'])
    infoProfissoes = infoProfissoes.groupby(['name','year','month'])['value'].sum().reset_index()

    if tipo == "Barras":
        figBarras = px.bar(infoProfissoes, x="name", y="value", color="name", title=titulo)

        figBarras.update_layout(
            xaxis=dict(title='Profissoes'),
            yaxis=dict(title='Valor Vendas')
            )


        grafico.plotly_chart(figBarras, use_container_width=True)
    
    if tipo == "Pizza":
        figPizza = px.pie(
        infoProfissoes,
        values="value",
        names="name",
        title=titulo,
        )
        grafico.plotly_chart(figPizza, use_container_width=True)

def getVendasProfissoesDataset(rd, titulo, dataInicial, dataFinal):
    chave = "Dashboard-vendas_profissoes"
    dados = getTrilha(rd, chave)
    infoProfissoes = pd.DataFrame(dados)

    anoInicial = int(dataInicial.split("/")[1])
    mesInicial = int(dataInicial.split("/")[0])
    anoFinal = int(dataFinal.split("/")[1])
    mesFinal = int(dataFinal.split("/")[0])

    infoProfissoes['date'] = pd.to_datetime(infoProfissoes[['year', 'month']].assign(DAY=1))
    filtro = infoProfissoes['date'].between(f"{anoInicial}-{mesInicial}", f"{anoFinal}-{mesFinal}")

    infoProfissoes = infoProfissoes[filtro]
    infoProfissoes = infoProfissoes.drop(columns=['date'])
    infoProfissoes = infoProfissoes.groupby(['name','year','month'])['value'].sum().reset_index()
    infoProfissoes.rename(columns={'name': 'Profissao', 'value': 'Valor Vendas','year': 'Ano', 'month': 'Mês'}, inplace=True)

    st.markdown(
        f"<h2 style='font-size:1em;'>{titulo} - de: {dataInicial} até: {dataFinal}</h2>",
        unsafe_allow_html=True
    )
    st.dataframe(infoProfissoes, width=800, height=400)
    st.markdown("---")

def getVendasTipoImovel(rd, grafico, titulo, tipo, dataInicial, dataFinal):
    chave = "Dashboard-vendas_tipo_imovel"
    dados = getTrilha(rd, chave)
    infoTipoImovel = pd.DataFrame(dados)

    anoInicial = int(dataInicial.split("/")[1])
    mesInicial = int(dataInicial.split("/")[0])
    anoFinal = int(dataFinal.split("/")[1])
    mesFinal = int(dataFinal.split("/")[0])

    infoTipoImovel['date'] = pd.to_datetime(infoTipoImovel[['year', 'month']].assign(DAY=1))
    filtro = infoTipoImovel['date'].between(f"{anoInicial}-{mesInicial}", f"{anoFinal}-{mesFinal}")

    infoTipoImovel = infoTipoImovel[filtro]
    infoTipoImovel = infoTipoImovel.drop(columns=['date'])
    # infoTipoImovel = infoTipoImovel.groupby(['name','year','month'])['value'].sum().reset_index()

    if tipo == "Barras":
        figBarras = px.bar(infoTipoImovel, x="name", y="value", color="name", title=titulo)

        figBarras.update_layout(
            xaxis=dict(title=titulo),
            yaxis=dict(title='Valor Vendas')
            )
    
        grafico.plotly_chart(figBarras, use_container_width=True)
    
    if tipo == "Pizza":
        figPizza = px.pie(
        infoTipoImovel,
        values="value",
        names="name",
        title=titulo,
        )
        grafico.plotly_chart(figPizza, use_container_width=True)

def getVendasTipoImovelDataset(rd, titulo, dataInicial, dataFinal):
    chave = "Dashboard-vendas_tipo_imovel"
    dados = getTrilha(rd, chave)
    infoTipoImovel = pd.DataFrame(dados)

    anoInicial = int(dataInicial.split("/")[1])
    mesInicial = int(dataInicial.split("/")[0])
    anoFinal = int(dataFinal.split("/")[1])
    mesFinal = int(dataFinal.split("/")[0])

    infoTipoImovel['date'] = pd.to_datetime(infoTipoImovel[['year', 'month']].assign(DAY=1))
    filtro = infoTipoImovel['date'].between(f"{anoInicial}-{mesInicial}", f"{anoFinal}-{mesFinal}")

    infoTipoImovel = infoTipoImovel[filtro]
    infoTipoImovel = infoTipoImovel.drop(columns=['date'])
    infoTipoImovel = infoTipoImovel.groupby(['name','year','month'])['value'].sum().reset_index()
    infoTipoImovel.rename(columns={'name': 'Tipo Imovel', 'value': 'Valor Vendas','year': 'Ano', 'month': 'Mês'}, inplace=True)

    st.markdown(
        f"<h2 style='font-size:1em;'>{titulo} - de: {dataInicial} até: {dataFinal}</h2>",
        unsafe_allow_html=True
    )
    st.dataframe(infoTipoImovel, width=800, height=400)

def getEmpreendQTY(rd, grafico, titulo, tipo, dataInicial, dataFinal):
    chave = "Dashboard-vendas_vgvempreendqty"
    dados = getTrilha(rd, chave)
    infoEmpreendimentos = pd.DataFrame(dados)
    
    infoEmpreendimentos['firstName'] = infoEmpreendimentos['enterpriseName'].str.split(' ').str[0]
    
    if tipo == "Barras":
        figBarras = px.bar(infoEmpreendimentos, x="enterpriseName", y="value", color="firstName", title=titulo)

        figBarras.update_layout(
            xaxis=dict(title='Empreendimento'),
            yaxis=dict(title='Valor Vendas')
            )

        grafico.plotly_chart(figBarras, use_container_width=True)
        return
            
    figBarras = px.pie(infoEmpreendimentos, values="value", names="enterpriseName", title=titulo)
    grafico.plotly_chart(figBarras, use_container_width=True)
    
def getEmpreendQTYDataset(rd, titulo, dataInicial, dataFinal):
    chave = "Dashboard-vendas_vgvempreendqty"
    dados = getTrilha(rd, chave)
    infoEmpreendimentos = pd.DataFrame(dados)
    
    st.markdown(
        f"<h2 style='font-size:1em;'>{titulo}</h2>",
        unsafe_allow_html=True
    )
    
    infoEmpreendimentos.rename(columns={'enterpriseName': 'Empreendimento', 'value': 'Valor Vendas'}, inplace=True)
    
    filtroDf = dataframe_explorer(infoEmpreendimentos)
    st.dataframe(filtroDf, width=800, height=400)
    st.markdown("---") 

def getEmpresaQTY(rd, grafico, titulo, tipo, dataInicial, dataFinal):
    chave = "Dashboard-vendas_vgvempresaqty"
    dados = getTrilha(rd, chave)
    infoEmpresas = pd.DataFrame(dados)
    
    infoEmpresas['firstName'] = infoEmpresas['companyName'].str.split(' ').str[0]
    
    if tipo == "Barras":
        figBarras = px.bar(infoEmpresas, x="firstName", y="value", color="firstName", title=titulo)

        figBarras.update_layout(
            xaxis=dict(title='Empresa', tickfont=dict(size=10)),  # Ajusta o tamanho do rótulo no eixo X
            yaxis=dict(title='Valor Vendas'),
            legend=dict(title='Empresas', font=dict(size=12))  # Ajusta o tamanho da legenda
        )

        grafico.plotly_chart(figBarras, use_container_width=True)
        return
            
    figBarras = px.pie(infoEmpresas, values="value", names="companyName", title=titulo)
    
    figBarras.update_layout(
        legend=dict(title='Empresas', font=dict(size=12))  # Ajusta o tamanho da legenda
    )
    
    grafico.plotly_chart(figBarras, use_container_width=True)
    
def getEmpresaQTYDataset(rd, titulo, dataInicial, dataFinal):
    chave = "Dashboard-vendas_vgvempresaqty"
    dados = getTrilha(rd, chave)
    infoEmpresas = pd.DataFrame(dados)
    
    st.markdown(
        f"<h2 style='font-size:1em;'>{titulo}</h2>",
        unsafe_allow_html=True
    )
    
    infoEmpresas.rename(columns={'companyName': 'Empresa', 'value': 'Valor Vendas'}, inplace=True)
    
    filtroDf = dataframe_explorer(infoEmpresas)
    
    st.dataframe(filtroDf, width=800, height=400)
    st.markdown("---")

def getVendasPeriodoQTY(rd, grafico, titulo, tipo, dataInicial, dataFinal):
    chave = "Dashboard-vendas_vgvperiodoqty"
    infoGrafico = getTrilha(rd, chave)
    infoGrafico = pd.DataFrame(infoGrafico)
    values = infoGrafico["value"]
    month = infoGrafico["month"]
    year = infoGrafico["year"]
    
    mesInicial = int(dataInicial.split("/")[0])
    anoInicial = int(dataInicial.split("/")[1])
    
    mesFinal = int(dataFinal.split("/")[0])
    anoFinal = int(dataFinal.split("/")[1])

    infoGrafico['date'] = pd.to_datetime(infoGrafico[['year', 'month']].assign(DAY=1))
    filtro = infoGrafico['date'].between(f"{anoInicial}-{mesInicial}", f"{anoFinal}-{mesFinal}")
    
    infoGrafico = infoGrafico[filtro]
    infoGrafico = infoGrafico.drop(columns=['date'])
    
    if tipo == "Linhas":
        figLinha = px.line(
            infoGrafico, x=month, y=values, title=titulo, color=year, labels={'year': 'Ano', 'month': 'Mês', 'value': 'Valor Vendas'}
        )
        grafico.plotly_chart(figLinha, use_container_width=True)
        return 

    cores = px.colors.qualitative.Set1
    figBarras = go.Figure()

    for i, year in enumerate(infoGrafico['year'].unique()):
        dados_ano = infoGrafico[infoGrafico['year'] == year]
        barras_ano = go.Bar(
            x=dados_ano['month'],
            y=dados_ano['value'],
            name=str(year),
            marker_color=cores[i]  # Atribui cores diferentes para cada ano
        )
        figBarras.add_trace(barras_ano)

    figBarras.update_layout(
        xaxis=dict(title='Mês'),
        yaxis=dict(title='Valor Vendas'),
        title=titulo,
        barmode='group'  # Agrupa as barras lado a lado
    )
    grafico.plotly_chart(figBarras, use_container_width=True)

def getVendasPeriodoQTYDataset(rd, titulo, dataInicial, dataFinal):
    chave = "Dashboard-vendas_vgvperiodoqty"
    infoGrafico = getTrilha(rd, chave)
    infoGrafico = pd.DataFrame(infoGrafico)
    infoGrafico.rename(columns={'value': 'Valor Vendas', 'year': 'Ano', 'month': 'Mês'}, inplace=True)
    
    st.markdown(
        f"<h2 style='font-size:1em;'>{titulo}</h2>",
        unsafe_allow_html=True
    )
    
    filtroDf = dataframe_explorer(infoGrafico)
    st.dataframe(filtroDf, width=800, height=400)
    st.markdown("---")

funcoes = {
    "Vendas por período": getVendasPeriodo,
    "Vendas por empresa": getVendasEmpresa,
    "Velocidade de Vendas": getVelocidadeVendas,
    "Estoque por Empreendimento": getEstoqueEmpreendimento,
    "Vendas por Corretor": getVendasCorretor,
    "Vendas m² Estoque": getVendasM2Estq,
    "Vendas por m²": getVendasM2,
    "Vendas por Idade": getVendasIdade,
    "Vendas por Cidade (Cliente)": getVendasClientesCidade,
    "Vendas por UF (Cliente)": getVendasClientesUF,
    "Vendas por Cidade (Empreendimentos)": getVendasEmpreedimentosCidades,
    "Vendas por UF (Empreendimentos)": getVendasEmpreedimentosUf,
    "Vendas por sexo": getVendasSexo,
    "Vendas por profissão": getVendasProfissoes,
    "Vendas por tipo de imóvel": getVendasTipoImovel,
    "Vendas por Empreendimento": getEmpreendQTY,
    "Qtd. Vendas por Empresa": getEmpresaQTY,
    "Qtd. Vendas por Período": getVendasPeriodoQTY
}

dataset = {
    "Vendas por período": getVendasPeriodoDataSet,
    "Vendas por empresa": getVendasEmpresaDataSet,
    "Velocidade de Vendas": getVelocidadeVendasDataSet,
    "Vendas por Corretor": getVendasCorretorDataset,
    "Estoque por Empreendimento": getEstoqueEmpreendimentoDataSet,
    "Vendas por Idade": getVendasIdadeDataset,
    "Vendas por Cidade (Cliente)": getVendasClientesCidadeDataset,
    "Vendas por UF (Cliente)": getVendasClientesUFDataset,
    "Vendas por Cidade (Empreendimentos)": getVendasEmpreedimentosCidadesDataset,
    "Vendas por UF (Empreendimentos)": getVendasEmpreedimentosUfDataset,
    "Vendas m² Estoque": getVendasM2EstqDataset,
    "Vendas por m²": getVendasM2Dataset,
    "Vendas por sexo": getVendasSexoDataset,
    "Vendas por profissão": getVendasProfissoesDataset,
    "Vendas por tipo de imóvel": getVendasTipoImovelDataset,
    "Vendas m² Estoque": getVendasM2EstqDataset,
    "Vendas por m²": getVendasM2Dataset,
    "Vendas por Empreendimento": getEmpreendQTYDataset,
    "Qtd. Vendas por Empresa": getEmpresaQTYDataset,
    "Qtd. Vendas por Período": getVendasPeriodoQTYDataset
}