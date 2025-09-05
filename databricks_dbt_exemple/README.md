## Instalação

Para instalar o dbt, basta rodar:

python -m pip install dbt-core dbt-databricks

## Variáveis de ambiente:

Para rodar localmente este projeto, é necessário setar as seguintes variáveis de ambiente:

TARGET: Catalogo que será utilizado para desenvolvimento (não utilizar o de produção)
HOST: Endereço do workplace do Databricks
HTTP_PATH: Endereço do cluster utilizado no Databricks, por padrão o projeto utiliza o X-Small
DBT_ACCESS_TOKEN: Token gerado através do Databricks para acesso via API

Para setar as variáveis, podemos utilizar os comandos (apenas usuários MAC):
echo "export TARGET='CATALOGO'" >> ~/.zshrc
echo "export HOST='#########.cloud.databricks.com'" >> ~/.zshrc
echo "export HTTP_PATH='/sql/1.0/warehouses/45e9fe979809a35a'" >> ~/.zshrc
echo "export DBT_ACCESS_TOKEN='TOKEN'" >> ~/.zshrc
source ~/.zshrc

Opcionalmente, pode-se criar um arquivo .env baseado no arquivo example.env no projeto:

export TARGET='CATALOGO'
export HOST='dbc-0c399854-e0b2.cloud.databricks.com'
export HTTP_PATH='/sql/1.0/warehouses/45e9fe979809a35a'
export DBT_ACCESS_TOKEN='TOKEN'

Uma vez preenchidas as variáveis, basta roda:

source .env

## Quickstart:

Para começar a usar o projeto:

1. Crie um catálogo no dbt com o format dev_{nome do dev}
2. Gere sua Token de Autenticação
3. Instale o dbt na máquina
4. Crie seu arquivo .env para carregar sua autenticação
5. Rode dbt seed para carregar as as sementes no seu catálogo
6. Rode dbt run para carregar o projeto no seu catálogo

