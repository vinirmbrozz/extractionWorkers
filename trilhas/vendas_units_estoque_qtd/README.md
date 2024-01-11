# Vendas Units Estoque

Este script Python se conecta a um serviço de mensageria, recupera dados de unidades de estoque e os processa.

## Dependências

- redis
- json
- pydantic
- Logger
- QueueExchange
- sys 
- time

## Como usar 

1. Importe as bibliotecas necessárias.
2. Veja se a imagem do `Redis` e do `HabbitMQ` já esta levantadas.
3. Defina a função `conectaLogger` para conectar ao serviço de log.
4. A função `start` é onde inicia o processamento. Ela se conecta ao Redis, recupera os dados das unidades de estoque e soma a quantidade de unidades da base de dados, processa esses dados e os armazena de volta no Redis.
5. A função `callback` é chamada quando uma mensagem é recebida do serviço de mensageria. Ela inicia o processamento dos dados e envia uma mensagem de volta ao serviço de mensageria quando termina.
6. Finalmente, o script tenta se conectar ao serviço de mensageria e começa a consumir mensagens.

## Execução
1.  O script tenta se conectar ao servidor RabbitMQ e inicializa a fila de mensagens `QueueExchange`.
2.  Se a conexão falhar, aguarda 20 segundos e encerra.
3.  Se bem-sucedido, inicia o consumo de mensagens da fila, acionando a função callback.

## Comandos de execução
1. ```bash 
    pip install --no-cache-dir -r requirements.txt
   ```
2. ```bash 
    docker-compose up -d
    ```

## Tratamento de Erros

1.  O script inclui mecanismos de tratamento de erros para registrar e lidar com exceções durante a execução.
2.  Se ocorrer um erro, ele registra os detalhes e envia uma mensagem de falha para a fila RabbitMQ.

## Observações

Este script foi projetado para ser executado continuamente, consumindo mensagens do serviço de mensageria e processando os dados das unidades de estoque conforme as mensagens são recebidas.

