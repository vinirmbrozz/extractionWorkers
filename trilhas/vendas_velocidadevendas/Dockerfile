FROM python:3.11-slim-bookworm

WORKDIR /app
RUN mkdir /app/logs
COPY requirements.txt ./
RUN  pip install --no-cache-dir -r requirements.txt
RUN apt update && apt install vim -y
COPY src .
COPY utils .

CMD [ "python", "vendas_velocidadevendas.py" ] 
