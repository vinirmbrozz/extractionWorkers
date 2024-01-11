FROM python:3.10-slim

WORKDIR /app
COPY requirements.txt ./
RUN  pip install --no-cache-dir -r requirements.txt
RUN apt update && apt install vim -y
COPY src .
COPY utils .

CMD [ "python", "vendas_vgvperiodo.py" ] 
