# SG_Intel Vendas

Sistema de Apoio Tático para Vendas Mercado Imobiliário

## Componentes

### Loader - Serviço para Extração de Dados da API Sienge

- Controle do número de atualizações na chave dashboard.atualizacoes
- Aguarda trabalho de extração na queue loader: 'dashboard:start'
- Dados de controle de extrações armazenados no SDW e configurados na chave sg_intel_client
- Não havendo atualizações disponíveis, sinaliza status do Dashboard como 'Atualizado'

### Intel - Serviço para Gestão da execução das trilhas de dados

- Envia trabalho para as trilhas cadastradas na chave intel_trilhas
- Faz a gestão da execução  das trilhas de análise
- Atualiza status do Dashboard na chave dashboard

### Trilhas - Serviço de análise para geração de informação

* Especializado em tipos de informação específicos
* Aguarda trabalho em uma fila
* Voltado para a geração dados para gráficos e datasets para apresentação
* Acionando pelo **Intel**, sinaliza 'start', 'success' ou 'failure'

### Dashboard - Serviço para Apresentação de Dashboards

- Dashboard interativos para consumo dos dados gerados

### Configurator - Serviço para manuteção das chaves

* Permite a alteração dos valores das chaves a partir de uma fila de dados no cloudamqp

## CONFIGURAÇÃO

### Linhas Gerais

1. Clonar repositório SG_Intel_Vendas do Github
2. Configurar dados do cliente editando as chaves no diretório redis_keys
3. Adicionar as chaves no Redis database
4. Gerar as Imagens
5. Executar o Sistema

### Instruções

* clonar o sistema em git@github.com:desenvolvimentopsa/SG_Intel_Vendas.git
* gerar as imagens com buildimages.sh
  * pode ser necessário remover as imagens atuais antes de gerar novas
  * caso as remova, fazer um prune do systema docker
* configure o serviço redis para atender na 127.0.0.1
  * *docker-compose up redis -d*
  * é possível que haja outro serviço redis já conectado na localhost então edite um canal ainda não utilizado para permitir o redirecionamento; ao terminar seja um devop clean e desative o serviço da localhost.
  * *docker-compose stop redis*
* gravar as chaves existentes no diretório redis_keys com o script
* subir os restante dos serviços e verificar a operação
* baixar o serviço e remover quais configurações relevantes ao processo de setup. Be a clean devops.
* abrir o dashboard utilizando o IP/Porta  configurados
* adicionar o cookie sistemas.token para testar o dashboard sem fazer o login no frontend PSA
