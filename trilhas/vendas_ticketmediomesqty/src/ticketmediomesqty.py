import redis
import json
import calendar
import pandas as pd
from datetime import datetime

# SEPARA O VGV, VALOR TOTAL DE CADA EMPREENDIMENTO

# VGV POR EMPREENDIMENTO
r = redis.Redis("localhost", 6379)
vgv_P = json.loads(r.get("trilha_vgvperiodoqty").decode())


i = 0
for valor in vgv_P:
    ultimo_dia = calendar.monthrange(valor["year"], valor["month"])[1]
    resultado = valor["value"] / ultimo_dia
    valor["value"] = resultado


r.set("trilha_ticketmediomesqty", str(vgv_P))
r.close()
