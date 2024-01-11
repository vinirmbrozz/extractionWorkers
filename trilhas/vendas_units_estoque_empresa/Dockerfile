FROM python:3.11-slim

WORKDIR /app
RUN mkdir /app/logs
COPY requirements.txt ./
RUN  pip install --no-cache-dir -r requirements.txt

COPY src .
COPY utils .

CMD [ "python", "vendas_units_estoque_empresa.py" ]