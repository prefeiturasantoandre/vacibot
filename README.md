# VACIBOT v.06 - Bot para automatizacao de registros no Vacivida para o COVID19

by Victor Fragoso - Prefeitura Municipal de Santo André

Email: vfragoso@santoandre.sp.gov.br

Phone: +55 11 97079 1572

Date: 24-mar-2021

Version: v.06


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

1. Instale os softwares

   obs.: Para o modo distribuído é necessário instalar em todas as máquinas do cluster

   ```
   sudo apt-get update
   sudo apt install -y python3 python3-pip ray
   ```

2. Instale os módulos Python através do PIP

   obs.: Para o modo distribuído é necessário instalar em todas as máquinas do cluster

   ```
   pip3 install ray cx_Oracle unidecode lxml unidecode requests
   ou
   python3 -m pip install cx_Oracle unidecode lxml unidecode requests
   ```

3. Faca o download do Instant Client OracleDB, converta e instale:

   obs.: Para o modo distribuído é necessário instalar em todas as máquinas do cluster

   ```
   wget https://download.oracle.com/otn_software/linux/instantclient/oracle-instantclient-basic-linuxx64.rpm
   sudo apt-get install alien libaio1
   sudo alien oracle-instantclientX.X-basic-x.x.x.x.x-1.amd64.rpm
   sudo dpkg -i oracle-instantclient-basic_x.x.x.x.x-2_amd64.deb
   ```

4. Altere as credenciais de acesso no arquivo `credentials.py` seguindo o padrão do arquivo

5. Adicione uma chave no `dictionary_vacivida.py` se houver um novo lote sendo aplicado de acordo com o vacivida
   Obs.: Para isso é necessário acessar o código HTML da página de vacinacao do vacivida e buscar quais chaves estão disponíveis. Você pode buscar diretamente pelo código de alguma chave já existente para encontrar mais facilmente.
   Irei adicionar um tutorial em breve sobre como fazer isso.

6. Altere a variavel `n_workers` para a quantidade de processadores disponíveis

7. Altere a variável `select_query` de acordo com a busca necessária em seu banco de dados para encontrar os nomes
   que precisam ser enviados para o Vacivida

8. Configure o Script para rodar em modo Standalone ou Distribuido

### 8.1 - Standalone:

  8.1.1 - Alterar a variavel `n_workers` para quantidade de processadores disponíveis no computador

  8.1.2 - Alterar a variavel `standalonemode` para `True`

### 8.2 - Distribuído:

  8.2.1 - Alterar a variavel `n_workers` para quantidade de processadores disponíveis na rede

  8.2.2 - Alterar a variavel `standalonemode` para `True`

  8.2.3 - Execute no terminal do computador principal o comando:

```
ray start --head --port=6379
```

8.2.4 - Execute em todos os computadores que estarão vinculados ao cluster o comando:

```
ray start --address='ip_do_node_principal:6379' --redis-password='5241590000000000'
```

8.2.5 - Verifique no computador principal se todos os nodes estao habilitados e conectados no cluster:

```
ray status
```

![Print do Ray Status](https://github.com/prefeiturasantoandre/vacibot/blob/main/images/ray_status.png)


9. Inicie o bot no computador principal com o comando:

 ```
 python3 vacibot_v06.py
 ```

![Print do Bot em Execucao 01](https://github.com/prefeiturasantoandre/vacibot/blob/main/images/print_final_01.png)
![Print do Bot em Execucao 02](https://github.com/prefeiturasantoandre/vacibot/blob/main/images/print_final_02.png)



Dicas:

1. Para realizar modificacoes, é possível converter os comandos CURL (que foram capturados com a ferramenta de
desenvolvedor do chrome) para Python através do link: https://curl.trillworks.com/#python

## License

This project is licensed under the MIT License - see the [LICENSE.md](LICENSE.md) file for details

