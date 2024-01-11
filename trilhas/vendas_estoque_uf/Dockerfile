FROM python:3.10-slim

WORKDIR /app
COPY requirements.txt ./
RUN  pip install --no-cache-dir -r requirements.txt

COPY src .
COPY utils .

CMD [ "python", "vendas_estoque_uf.py" ]