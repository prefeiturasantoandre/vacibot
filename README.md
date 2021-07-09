# VACIBOT v.08 - Bot para automatizacao de registros no Vacivida para o COVID19

by [Victor Fragoso](https://github.com/victorffs), [Willian Sanches](https://github.com/wi-sanc) - Prefeitura Municipal de Santo André

Date: 09-jul-2021

Version: v.08


![Print do Vacivida](https://github.com/prefeiturasantoandre/vacibot/blob/main/images/print_vacinometro.jpeg)


## O QUE É? 

Este script em python tem como objetivo eliminar a necessidade de digitacão manual no sistema de vacinacão do
COVID19 do Governo do Estado de Sao Paulo chamado "Vacivida" (https://vacivida.sp.gov.br/).

Como o Vacivida nao possui nenhuma forma de integracão com os bancos de dados das prefeituras dos municipios,
nem a possibilidade de fazer upload de uma tabela em Excel, o script se mostrou extremamente importante para
agilizar a atualizacao no ranking do Governo do Estado (https://vacinaja.sp.gov.br/vacinometro/) no qual
também é utilizado para controle de quais cidades estarão mais propensas a receber mais vacinas, uma vez que
no mesmo consta a quantidade de doses enviadas e quantidade de doses aplicadas em cada município,
e espera-se que a quantidade de doses aplicadas seja igual ou proximo a quantidade de doses enviadas.

O uso desse script permitiu o que o município de Santo Andre pulasse de 64.419 registros no dia 17/03/2021,
para 94.133 no dia 24/03/2021 (ou seja, quase 30 mil registros) sem a necessidade de nenhuma equipe de digitacao,
permitindo assim que esses trabalhadores pudessem se preocupar em realizar tarefas de maior valor agregado,
como a própria aplicacao de vacinas contra o COVID19.

Esse código é livre, aberto, e qualquer um pode realizar as modificacoes que acharem necessários para implementar
em seus próprios sistemas.

Eu espero de coracão que esse codigo possa ajudar de alguma forma na vacinacao do Brasil. <3

Sinta-se a vontade para me contatar se precisar de qualquer auxílio.

## CONSIDERACOES TÉCNICAS

- O uso desse script tem como prerrogativa que o município já possui um sistema interno de controle de vacinacao, com formulário de inscricão dos munícipes e as informacões "check-in" sobre quem tomou as vacinas em um banco de dados.

- Caso não haja um formulário já existente, é possível que os registros sejam incluídos dentro de um banco de dados e exportados através do bot, bastando alterar as colunas utilizadas no script.

- O banco de dados utilizado pela Prefeitura de Santo André é o Oracle, e por isso o bot foi feito integrado nele. Mas pode ser portado para qualquer outro banco disponível, bastando alterar as depenedencias e funcões.

- Por conta do grande volume de dados, o bot foi construído com base em computacão distribuída através do uso da biblioteca Python Ray.io (https://ray.io/) em 36 computadores (com 4 núcleos cada) ligados dentro de uma mesma rede ethernet, formando um cluster com 144 "workers" disponíveis para a automatizacao.

- Caso não tenha disponível tantos computadores, é possível rodar individualmente o script em um único computador (modo standalone), bastando substituir a quantidade de "workers" por núcleos do processador disponível.

## INSTRUCÕES 

1. Instale os softwares necessários

   obs.: Para o modo distribuído é necessário instalar em todas as máquinas do cluster

   ```
   sudo apt-get update
   sudo apt install -y python3 python3-pip ray
   ```

2. Clone o repositório do github e acesse a pasta

   ```
   git clone https://github.com/prefeiturasantoandre/vacibot.git
   cd vacibot/
   ```

3. (opcional) Crie um Virtual Environment e ative-o 


   obs.: Será necessário ativá-lo sempre que for utilizar o bot

   ```
   python3 -m venv venv
   . venv/bin/activate
   ```

4. Instale os módulos Python através do PIP

   obs.: Para o modo distribuído é necessário instalar em todas as máquinas do cluster

   ```
   pip3 install -r requirements.txt
   ou
   python3 -m pip install -r requirements.txt
   ```

5. Faca o download do Instant Client OracleDB, converta e instale:

   obs.: Para o modo distribuído é necessário instalar em todas as máquinas do cluster

   ```
   wget https://download.oracle.com/otn_software/linux/instantclient/oracle-instantclient-basic-linuxx64.rpm
   sudo apt-get install alien libaio1
   sudo alien oracle-instantclientX.X-basic-x.x.x.x.x-1.amd64.rpm
   sudo dpkg -i oracle-instantclient-basic_x.x.x.x.x-2_amd64.deb
   ```

6. Altere as credenciais de acesso no arquivo `credentials.py` seguindo o padrão do arquivo

Obs: Caso utilize o git para controle local, recomenda-se ignorar o arquivo ao realizar algum push. Para isso, acrescente `credentials.py` ao final do arquivo `.gitignore`

7. (opcional) Altere a variavel `n_workers` de acordo com a quantidade de processadores disponíveis dividido pela quantidade de locais de vacinação. 

8. Altere a variável `select_query` em `__main__.py` de acordo com a busca necessária em seu banco de dados para encontrar os nomes
   que precisam ser enviados para o Vacivida

9.  Preencha os dicionários `area_alias`, `vacinador`, `estabelecimento` em `dicts.py` de acordo com as informações das unidades.

10. Configure o Script para rodar em modo Standalone ou Distribuido

### 10.1 - Standalone:

  10.1.1 - Alterar a variavel `n_workers` para quantidade de processadores disponíveis no computador dividido pela quantidade de locais de vacinação

  10.1.2 - Alterar a variavel `standalonemode` para `True`

### 10.2 - Distribuído:

  10.2.1 - Alterar a variavel `n_workers` para quantidade de processadores disponíveis na rede dividido pela quantidade de locais de vacinação

  10.2.2 - Alterar a variavel `standalonemode` para `True`

  10.2.3 - Execute no terminal do computador principal o comando:

```
ray start --head --port=6379
```

10.2.4 - Execute em todos os computadores que estarão vinculados ao cluster o comando:

```
ray start --address='ip_do_node_principal:6379' --redis-password='5241590000000000'
```

10.2.5 - Verifique no computador principal se todos os nodes estao habilitados e conectados no cluster:

```
ray status
```

![Print do Ray Status](https://github.com/prefeiturasantoandre/vacibot/blob/main/images/ray_status.png)


11. A partir da pasta anterior à pasta em que o bot está instalada, inicie o bot no computador principal com o comando:

 ```
 python3 vacibot
 ou
 python3 vacibot [N_WORKERS]
 ```

 onde [N_WORKERS] é a quantidade de workers desejada. Caso não especificado, utilizará o número pardão ou aquele especificado no passo 7.

 Obs.: Caso esteja utilizando o Virtual Envirenment, ative-o antes de executar o comando anterior:
 ```
 . vacibot/venv/bin/activate
 ```

![Print do Bot em Execucao 01](https://github.com/prefeiturasantoandre/vacibot/blob/main/images/print_final_01.png)
![Print do Bot em Execucao 02](https://github.com/prefeiturasantoandre/vacibot/blob/main/images/print_final_02.png)



Dicas:

1. Para realizar modificacoes, é possível converter os comandos CURL (que foram capturados com a ferramenta de
desenvolvedor do chrome) para Python através do link: https://curl.trillworks.com/#python

## Autores
### Victor Fragoso
Github: [victorffs](https://github.com/victorffs)

Email: vfragoso@santoandre.sp.gov.br

Phone: +55 11 97079 1572
### Willian Sanches
Github: [wi-sanc](https://github.com/wi-sanc)

Email: wcsanches@santoandre.sp.gov.br

Phone: +55 11 97707 4734

## License

This project is licensed under the MIT License - see the [LICENSE.md](LICENSE.md) file for details

