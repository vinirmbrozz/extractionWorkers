import redis
import json
import calendar
import pandas as pd
from datetime import datetime

# SEPARA O VGV, VALOR TOTAL DE CADA EMPREENDIMENTO

# VGV POR EMPREENDIMENTO
r = redis.Redis("localhost", 6379)
units_d_mes = json.loads(r.get("loader_vendas_units_estoque").decode())
contratos_E = json.loads(r.get("loader_sales_contracts_emitido").decode())
contratos_C = json.loads(r.get("loader_sales_contracts_cancelado").decode())

contratos = contratos_E + contratos_C
df = pd.DataFrame(contratos)
for valor in units_d_mes:
    for valorC in contratos:
        if valorC["id"] == valor["contractId"]:
            df["contractDate"] = pd.to_datetime(df["contractDate"])
            df["month"] = df["contractDate"].dt.month
            df["year"] = df["contractDate"].dt.year
            # # Extract year and month from "contractDate"
            
            all_months = pd.DataFrame(
                {
                    "year": [datetime.now().year - 1] * 12 + [datetime.now().year] * 12,
                    "month": list(range(1, 13)) * 2,
                }
            )
            # Merge dataframes
            resultado = df.groupby(["year", "month"])["value"].count().reset_index()
            saida = resultado.to_json(orient="records")
            print(saida)
# r.set("trilha_ticketmediomesqty", str(vgv_P))
# r.close()
