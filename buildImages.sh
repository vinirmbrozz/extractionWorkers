version=':v1' # Versao Inicial
version=':v2' # Adicionado Códigos auxliares diretamente do Github e auto cnf docker-compose.yml

echo "TROCAR A VERSAO DAS IMAGENS EM docker-compose.yml"
sed -i'' -e 's/:v1/:v2/g' docker-compose.yml

echo "GERAR IMAGENS SG_INTEL_VENDAS ${version}"

echo "CLASSES ACESSORIAS Logger Exchange e Configurator"
if [ -d "utils" ]; then
  rm -rf utils
fi
git clone git@github.com:desenvolvimentopsa/Logger.git utils/Logger
git clone git@github.com:desenvolvimentopsa/Exchange.git utils/Exchange
git clone git@github.com:desenvolvimentopsa/Configurator.git utils/Configurator
cd utils
find . -name "*.py" | xargs -I {} cp {} .
rm -rf Logger Exchange Configurator
cd ..

cd loader
cp -r ../utils .
echo LOADER
docker build -t sg_intel_vendas-loader${version} .
cd ..
rm -rf loader/utils

cd intel
cp -r ../utils .
echo INTEL
docker build -t sg_intel_vendas-intel${version} .
cd ..
rm -rf intel/utils

cd dashboard
cp -r ../utils .
echo DASHBOARD
docker build -t sg_intel_vendas-dashboard${version} .
cd ..
rm -rf dashboard/utils

######################### TRILHAS

echo VENDAS_VELOCIDADE_VENDAS
cd trilhas/vendas_velocidadevendas
cp -r ../../utils .
docker build -t sg_intel_vendas-vendas_velocidadevendas${version} .
cd ../..
rm -rf trilhas/vendas_velocidadevendas/utils

echo VENDAS_VGVEMPRESA
cd trilhas/vendas_vgvempresa
cp -r ../../utils .
docker build -t sg_intel_vendas-vendas_vgvempresa${version} .
cd ../..
rm -rf trilhas/vendas_vgvempresa/utils

echo VENDAS_VGVPERIODO
cd trilhas/vendas_vgvperiodo
cp -r ../../utils .
docker build -t sg_intel_vendas-vendas_vgvperiodo${version} .
cd ../..
rm -rf trilhas/vendas_vgvperiodo/utils

echo VENDAS_UNITS_ESTOQUE_EMPREENDIMENTO
cd trilhas/vendas_units_estoque_empreendimento
cp -r ../../utils .
docker build -t sg_intel_vendas-vendas_units_estoque_empreendimento${version} .
cd ../..
rm -rf trilhas/vendas_units_estoque_empreendimento/utils

echo VENDAS_CORRETOR
cd trilhas/vendas_corretor
cp -r ../../utils .
docker build -t sg_intel_vendas-vendas_corretor${version} .
cd ../..
rm -rf trilhas/vendas_corretor/utils

echo VENDAS_VGV
cd trilhas/vendas_vgv
cp -r ../../utils .
docker build -t sg_intel_vendas-vendas_vgv${version} .
cd ../..
rm -rf trilhas/vendas_vgv/utils

echo VENDAS_UNITS_ESTOQUE_EMPRESA
cd trilhas/vendas_units_estoque_empresa
cp -r ../../utils .
docker build -t sg_intel_vendas-vendas_units_estoque_empresa${version} .
cd ../..
rm -rf trilhas/vendas_units_estoque_empresa/utils

echo VENDAS_M2_ESTQ
cd trilhas/vendas_m2_estq
cp -r ../../utils .
docker build -t sg_intel_vendas-vendas_m2_estq${version} .
cd ../..
rm -rf trilhas/vendas_m2_estq/utils


echo VENDAS_M2
cd trilhas/vendas_m2
cp -r ../../utils .
docker build -t sg_intel_vendas-vendas_m2${version} .
cd ../..
rm -rf trilhas/vendas_m2/utils

echo VENDAS_VGVEMPRESA
cd trilhas/vendas_vgvempresa
cp -r ../../utils .
docker build -t sg_intel_vendas-vendas_vgvempresa${version} .
cd ../..
rm -rf trilhas/vendas_vgvempresa/utils

echo VENDAS_CORRETOR
cd trilhas/vendas_corretor
cp -r ../../utils .
docker build -t sg_intel_vendas-vendas_corretor${version} .
cd ../..
rm -rf trilhas/vendas_corretor/utils

echo VENDAS_CUSTOMERS_CIDADE
cd trilhas/vendas_customers_cidade
cp -r ../../utils .
docker build -t sg_intel_vendas-vendas_customers_cidade${version} .
cd ../..
rm -rf trilhas/vendas_customers_cidade/utils

echo VENDAS_ENTERPRISES_CIDADES
cd trilhas/vendas_enterprises_cidade
cp -r ../../utils .
docker build -t sg_intel_vendas-vendas_enterprises_cidade${version} .
cd ../..
rm -rf trilhas/vendas_enterprises_cidade/utils

echo VENDAS_ESTOQUE_UF
cd trilhas/vendas_estoque_uf
cp -r ../../utils .
docker build -t sg_intel_vendas-vendas_estoque_uf${version} .
cd ../..
rm -rf trilhas/vendas_estoque_uf/utils

echo VENDAS_IDADE
cd trilhas/vendas_idade
cp -r ../../utils .
docker build -t sg_intel_vendas-vendas_idade${version} .
cd ../..
rm -rf trilhas/vendas_idade/utils

echo VENDAS_PROFISSAO
cd trilhas/vendas_profissao
cp -r ../../utils .
docker build -t sg_intel_vendas-vendas_profissao${version} .
cd ../..
rm -rf trilhas/vendas_profissao/utils

echo VENDAS_SEXO
cd trilhas/vendas_sexo
cp -r ../../utils .
docker build -t sg_intel_vendas-vendas_sexo${version} .
cd ../..
rm -rf trilhas/vendas_sexo/utils

echo VENDAS_TIPO_IMOVEL
cd trilhas/vendas_tipo_imovel
cp -r ../../utils .
docker build -t sg_intel_vendas-vendas_tipo_imovel${version} .
cd ../..
rm -rf trilhas/vendas_tipo_imovel/utils

echo VENDAS_EMPREENDQTY
cd trilhas/vendas_vgvempreendqty
cp -r ../../utils .
docker build -t sg_intel_vendas-vendas_vgvempreendqty${version} .
cd ../..
rm -rf trilhas/vendas_vgvempreendqty/utils

echo VENDAS_VGVPERIODOQTY
cd trilhas/vendas_vgvperiodoqty
cp -r ../../utils .
docker build -t sg_intel_vendas-vendas_vgvperiodoqty${version} .
cd ../..
rm -rf trilhas/vendas_vgvperiodoqty/utils

echo VENDAS_EMPRESADQTY
cd trilhas/vendas_vgvempresaqty
cp -r ../../utils .
docker build -t sg_intel_vendas-vendas_vgvempresaqty${version} .
cd ../..
rm -rf trilhas/vendas_vgvempresaqty/utils