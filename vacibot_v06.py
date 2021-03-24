'''
VACIBOT v.06 - Bot para automatizacao de registros no Vacivida para o COVID19
by Victor Fragoso - Prefeitura Municipal de Santo André
Email: vfragoso@santoandre.sp.gov.br
Phone: +55 11 97079 1572
Date: 24-mar-2021
Version: v.06
License: MIT License

INSTRUCÕES ---------------------------------------------------------

1. Instale os softwares
sudo apt-get update
sudo apt install -y python3 python3-pip ray
obs.: Para o modo distribuído é necessário instalar em todas as máquinas do cluster

2. Instale os módulos Python através do PIP
pip3 install ray cx_Oracle unidecode lxml unidecode requests
#ou
python3 -m pip install cx_Oracle unidecode lxml unidecode requests
obs.: Para o modo distribuído é necessário instalar em todas as máquinas do cluster

3. Faca o download do Instant Client OracleDB, converta e instale:
wget https://download.oracle.com/otn_software/linux/instantclient/oracle-instantclient-basic-linuxx64.rpm
sudo apt-get install alien libaio1
sudo alien oracle-instantclientX.X-basic-x.x.x.x.x-1.amd64.rpm
sudo dpkg -i oracle-instantclient-basic_x.x.x.x.x-2_amd64.deb
obs.: Para o modo distribuído é necessário instalar em todas as máquinas do cluster

4. Altere as credenciais de acesso no arquivo credentials.py

5. Adicione uma chave no dictionary_vacivida.py se houver um novo lote sendo aplicado de acordo com o vacivida
Obs.: Para isso é necessário acessar o código HTML da página de vacinacao do vacivida e buscar quais chaves estão
disponíveis. Você pode buscar diretamente pelo código de alguma chave já existente para encontrar mais facilmente.
Irei adicionar um tutorial em breve sobre como fazer isso.

6. Altere a variavel n_workers para a quantidade de processadores disponíveis

7. Altere a variável select_query de acordo com a busca necessária em seu banco de dados para encontrar os nomes
que precisam ser enviados para o Vacivida

8. Configure o Script para rodar em modo Standalone ou Distribuido
8.1 - Standalone:
    8.1.1. - Alterar a variavel n_workers para quantidade de processadores disponíveis no computador
    8.1.2. - Alterar a variavel standalonemode para True

8.2 - Distribuído:
    8.2.1. - Alterar a variavel n_workers para quantidade de processadores disponíveis na rede
    8.2.2. - Alterar a variavel standalonemode para True
    8.2.3. - Execute no terminal do computador principal o comando:
            ray start --head --port=6379
    8.2.4. - Execute em todos os computadores que estarão vinculados ao cluster o comando:
            ray start --address='ip_do_node_principal:6379' --redis-password='5241590000000000'
    8.2.5 - Verifique no computador principal se todos os nodes estao habilitados e conectados no cluster:
            ray status

9. Inicie o bot no computador principal com o comando:
    python3 vacibot_v06.py

Dicas:
1- Para realizar modificacoes, é possível converter os comandos CURL (que foram capturados com a ferramenta de
desenvolvedor do chrome) para Python através do link: https://curl.trillworks.com/#python


'''

#Importacao de bibliotecas Python:
import ray
import time
import numpy as np
import json
import requests
from unidecode import unidecode
from datetime import datetime, timedelta
import cx_Oracle
import random

#Importacao de arquivos de configuracao:
from credentials import connection_params
from credentials import login_vacivida
from dictionary_vacivida import lotes_astrazeneca
from dictionary_vacivida import lotes_coronavac

#DEFINES
n_workers=6 #recomendado nao passar de 40 workers por credentials.py
MAX_RETRY = 5 #attempts
standalonemode= True
headnodeip='127.0.0.1:6379'
headnodepassword='5241590000000000'

#Global variables
table_index=[]
cadastros= []
global list_to_send
global list_seq_agenda
global registers_to_send
list_to_send = []
list_seq_agenda = []
registers_to_send = []

#Precisa adicionar na funcao parse_to_dict() as keys caso sejam adicionadas novas
#SELECT:
select_query= "SELECT\
    a.seq_agenda,\
    r.dsc_area,\
    c.dsc_nome,\
    c.num_cpf,\
    c.dsc_nome_mae,\
    c.num_cns,\
    c.dta_nascimento,\
    c.num_telefone,\
    c.dsc_email,\
    c.dsc_endereco,\
    c.num_cep,\
    c.tpo_sexo,\
    t.dsc_raca_cor,\
    c.dsc_orgao_classe,\
    c.num_registro_classe,\
    a.dta_comparecimento_pessoa,\
    c.num_lote_vacina,\
    c.dsc_tipo_vacina,\
    c.num_dose_vacina,\
    c.ind_vacivida_cadastro,\
    c.ind_vacivida_vacinacao\
    FROM age_agenda a,\
    age_servico s,\
    age_agendamento_covid c,\
    age_area r,\
    age_raca_cor t\
    WHERE a.cod_area = s.cod_area\
    AND c.seq_agenda = a.seq_agenda\
    AND a.cod_area = r.cod_area\
    AND t.cod_raca_cor = c.cod_raca_cor\
    AND a.cod_pessoa IS NOT NULL\
    AND c.ind_vacivida_vacinacao is null\
    AND a.dta_comparecimento_pessoa is not null\
    ORDER BY c.seq_agenda DESC"

#    and num_lote_vacina is not null \
#    AND a.dta_comparecimento_pessoa >= '01-FEB-2021'\


#OPCAO PARA STANDALONE OU DISTRIBUIDO
if standalonemode:
    ray.init()
else:
    ray.init(address=headnodeip, _redis_password=headnodepassword)

#inicializa listas vazias
def GetDB():
    #limpa as listas
    list_to_send.clear()
    list_seq_agenda.clear()

    ####query que salva e envia TODAS as colunas
    con = cx_Oracle.connect(connection_params)
    cur=con.cursor()
    cur.execute(f'{select_query}')
    #registra indexes
    for i in cur.description :
        #print(i[0])
        table_index.append(i[0])
    for result in cur:
        list_seq_agenda.append(result)
        pass
    con.close()


    lista_dividida=np.array_split(list_seq_agenda,n_workers) #divide a lista de seqs pela quantidade de workers

    #Coloca o resultado do split em list_to_send de acordo com a qntde de workers
    for array in lista_dividida:
        #print (array)
        list_to_send.append(array.tolist())

    #tamanho da lista sera utilizado para definir o tamanho do loop dos workers
    size_of_list=len(list_seq_agenda)/n_workers

    print ("########## Tamanho da lista: ", len(list_seq_agenda))

#Cria classe RegisterBatch para poder realizar operacoes em cima de forma mais facil
class RegisterBatch():
    def __init__(self,list_agenda,list_index):
        self.list_agenda=list_agenda
        self.list_index=list_index

    def parse_to_dict(self,):
        self.list_agenda_parsed = []
        for list_agenda_line in self.list_agenda:
            self.dict={self.list_index[0]: str(list_agenda_line[0]),  #SEQ_AGENDA
                  self.list_index[1]: str(list_agenda_line[1]),  #'DSC_AREA'
                  self.list_index[2]: str(list_agenda_line[2]),  #'DSC_NOME'
                  self.list_index[3]: str(list_agenda_line[3]),  #'NUM_CPF'
                  self.list_index[4]: str(list_agenda_line[4]),  #'DSC_NOME_MAE'
                  self.list_index[5]: str(list_agenda_line[5]),  #'NUM_CNS'
                  self.list_index[6]: list_agenda_line[6],  #'DTA_NASCIMENTO'
                  self.list_index[7]: str(list_agenda_line[7]),  #'NUM_TELEFONE'
                  self.list_index[8]: str(list_agenda_line[8]),  #'DSC_EMAIL'
                  self.list_index[9]: str(list_agenda_line[9]),  #'DSC_ENDERECO'
                  self.list_index[10]: str(list_agenda_line[10]),  #'NUM_CEP'
                  self.list_index[11]: str(list_agenda_line[11]),  #'TPO_SEXO'
                  self.list_index[12]: str(list_agenda_line[12]),  #'DSC_RACA_COR'
                  self.list_index[13]: str(list_agenda_line[13]),  #'DSC_ORGAO_CLASSE'
                  self.list_index[14]: str(list_agenda_line[14]),  #'NUM_REGISTRO_CLASSE'
                  self.list_index[15]: list_agenda_line[15],  #'DTA_COMPARECIMENTO_PESSOA'
                  self.list_index[16]: str((list_agenda_line[16])),  #'NUM_LOTE_VACINA'
                  self.list_index[17]: str(list_agenda_line[17]),  #'DSC_TIPO_VACINA'
                  self.list_index[18]: str(list_agenda_line[18]),  #'NUM_DOSE_VACINA'
                  self.list_index[19]: str(list_agenda_line[19]),  #'IND_VACIVIDA_CADASTRO'
                  self.list_index[20]: str(list_agenda_line[20]),  #'IND_VACIVIDA_VACINACAO'
                  }

            #OTHERS KEYS:
            #GESTANTE
            #PUERPERA
            #TELDDD
            #TELNUM

            #parse CPF
            # adiciona zero a esquerda quando CPF tiver 10 digitos
            if (len(self.dict['NUM_CPF']) == 11) :
                #print ("DEBUG - CPF possui 11 digitos. CPF = " + self.dict['NUM_CPF'])
                pass
            elif (len(self.dict['NUM_CPF']) == 10) :
                self.dict['NUM_CPF'] = self.dict['NUM_CPF'][:0]+'0'+self.dict['NUM_CPF'][0 :]
            elif (len(self.dict['NUM_CPF']) == 9) :
                self.dict['NUM_CPF'] = self.dict['NUM_CPF'][:0]+'00'+self.dict['NUM_CPF'][0 :]
            elif (len(self.dict['NUM_CPF']) == 8) :
                self.dict['NUM_CPF'] = self.dict['NUM_CPF'][:0]+'000'+self.dict['NUM_CPF'][0 :]
            elif (len(self.dict['NUM_CPF']) == 7) :
                self.dict['NUM_CPF'] = self.dict['NUM_CPF'][:0]+'0000'+self.dict['NUM_CPF'][0 :]
            else:
                print ("CPF invalido! CPF = " + self.dict['NUM_CPF'])

            #parse nascimento
            self.dict['DTA_NASCIMENTO'] = self.dict['DTA_NASCIMENTO'].strftime("%Y-%m-%dT%H:%M:%S.000Z")


            # necessario fazer normalizacao dos caracteres removendo acentuacao com unidecode
            self.dict['DSC_NOME'] = unidecode(self.dict['DSC_NOME']).upper()
            self.dict['DSC_NOME_MAE'] = unidecode(self.dict['DSC_NOME_MAE']).upper()
            self.dict['DSC_ENDERECO'] = unidecode(self.dict['DSC_ENDERECO']).upper()

            self.dict['TPO_SEXO'] = "I"  # usar sexo indefinido pois a nossa tabela nao esta pedindo essa info

            # Consulta raca por tags
            if (self.dict['DSC_RACA_COR'] == "Amarela" or "AMARELA" or "amarela" or "Amarelo" or "AMARELO" or "amarelo") :
                self.dict['DSC_RACA_COR'] = "B66C4B622EF3840AE053D065C70A17A1"
            elif (self.dict['DSC_RACA_COR'] == "Branca" or "BRANCA" or "branca" or "Branco" or "BRANCO" or "branco") :
                self.dict['DSC_RACA_COR'] = "B66C4B622EF4840AE053D065C70A17A1"
            elif (self.dict['DSC_RACA_COR'] == "Indigena" or "INDIGENA" or "Indígena" or "INDÍGENA" or "indigena") :
                self.dict['DSC_RACA_COR'] = "B66C4B622EF7840AE053D065C70A17A1"
            elif (self.dict['DSC_RACA_COR'] == "Não Informada" or "NÃO INFORMADA" or "não informada" or "nao informada") :
                self.dict['DSC_RACA_COR'] = "B66C4B622EF8840AE053D065C70A17A1"
            elif (self.dict['DSC_RACA_COR'] == "Negra" or "NEGRA" or "negra" or "Negro" or "NEGRO" or "negro") :
                self.dict['DSC_RACA_COR'] = "B66C4B622EF5840AE053D065C70A17A1"
            elif (self.dict['DSC_RACA_COR'] == "Parda" or "PARDA" or "parda" or "Pardo" or "PARDO" or "pardo") :
                self.dict['DSC_RACA_COR'] = "B66C4B622EF6840AE053D065C70A17A1"

            # adiciona 0 ao numero de telefone se for fixo para passar na validacao de 9 digitos
            if (len(self.dict['NUM_TELEFONE']) == 10) :
                self.dict['NUM_TELEFONE'] = self.dict['NUM_TELEFONE'][:2]+'0'+self.dict['NUM_TELEFONE'][2 :]

            self.dict['NUM_TELEFONE_DDD'] = self.dict['NUM_TELEFONE'][0 :2]
            self.dict['NUM_TELEFONE_NUM'] = self.dict['NUM_TELEFONE'][2 :len(self.dict['NUM_TELEFONE'])]

            #fixed values
            self.dict['GESTANTE'] = "false"
            self.dict['PUERPERA'] = "false"
            self.dict['PAIS'] = "B66C4B622E1B840AE053D065C70A17A1"
            self.dict['UF'] = "B66C4B622DF6840AE053D065C70A17A1"
            self.dict['MUNICIPIO'] = "B66C4B623E5E840AE053D065C70A17A1"
            self.dict['ZONA'] = "U"
            self.dict['NUM_ENDERECO'] = "1"  # precisa criar parser
            self.dict['BAIRRO'] = "bairro"  # precisa criar parser
            self.dict['COMPLEMENTO'] = "complemento"


            #### INFOS PARA VACINACAO
            # fixos:
            self.dict['ESTRATEGIA']="B66C4B622F6E840AE053D065C70A17A1"
            self.dict['VIA_ADMINISTRACAO'] = "B66C4B622F6B840AE053D065C70A17A1"
            self.dict['LOCAL_APLICACAO'] = "B66C4B622F25840AE053D065C70A17A1"
            self.dict['VACINADOR']= "26749734-6e50-42aa-bde8-cd29266fb3a2"
            self.dict['ESTABELECIMENTO']= "B6706DE0D6541995E053D065C70A4952"

            #print(self.dict['DSC_AREA'])

            # parse grupo
            if ("IDOSOS" in self.dict['DSC_AREA']):
                self.dict['DSC_AREA'] = "BA0E494756847EBFE053D065C70AE389"
            elif ("SAÚDE" in self.dict['DSC_AREA']) :
                self.dict['DSC_AREA'] = "B83C80018F62B8B5E053D065C70AB1BB"
            else:
                print ("Grupo de vacinacao nao identificado!" , self.dict['DSC_AREA'])

            # parse dose
            if ('1' in self.dict['NUM_DOSE_VACINA']) :
                self.dict['NUM_DOSE_VACINA'] = "B66C4B622F1F840AE053D065C70A17A1"
            elif ('2' in self.dict['NUM_DOSE_VACINA']) :
                self.dict['NUM_DOSE_VACINA'] = "B66C4B622F21840AE053D065C70A17A1"

            #parse tipo de vacina
            if ("CORONAVAC"in self.dict['DSC_TIPO_VACINA'] or "Coronavac"in self.dict['DSC_TIPO_VACINA']):
                self.dict['DSC_TIPO_VACINA'] = "b309a279-f8b8-4023-b393-ed32723127ea"
            elif ("AstraZeneca" in self.dict['DSC_TIPO_VACINA'] or "Oxford" in self.dict['DSC_TIPO_VACINA']) :
                self.dict['DSC_TIPO_VACINA'] = "B9BAB192F78A60DBE053D065C70A09F3"

            #formata lote
            if (self.dict['DSC_TIPO_VACINA'] == 'b309a279-f8b8-4023-b393-ed32723127ea') :
                # print ("consulta tabela de lotes coronavac")
                self.dict['NUM_LOTE_VACINA'] = lotes_coronavac.get(self.dict['NUM_LOTE_VACINA'])
            elif (self.dict['DSC_TIPO_VACINA'] == 'B9BAB192F78A60DBE053D065C70A09F3') :
                # print("consulta tabela de lotes astrazeneca")
                self.dict['NUM_LOTE_VACINA'] = lotes_astrazeneca.get(self.dict['NUM_LOTE_VACINA'])

            # calcula aprazamento e parser datas
            #print ("DEBUG - Data comparecimento = ", self.dict['DTA_COMPARECIMENTO_PESSOA'])

            # coronavac
            if (self.dict['DSC_TIPO_VACINA'] == "b309a279-f8b8-4023-b393-ed32723127ea") :
                #datetime_object = datetime.strptime(self.dict['DTA_COMPARECIMENTO_PESSOA'], '%Y-%m-%dT%H:%M:%S.000Z')
                datetime_object = self.dict['DTA_COMPARECIMENTO_PESSOA']
                # print(datetime_object)
                somadatas = (datetime_object+timedelta(days=28))
                # print(somadatas)
                somadatas = somadatas.strftime("%Y-%m-%dT%H:%M:%S")
                #print ('SOMADATAS CORONAVAC = ' + somadatas)

                self.dict['DTA_APRAZAMENTO'] = str(somadatas+".000Z")
                #print ("Data aprazamento parseado = " + self.dict['DTA_APRAZAMENTO'])
                #print("data aprazamento "+somadatas)

            # astrazeneca
            elif (self.dict['DSC_TIPO_VACINA'] == "B9BAB192F78A60DBE053D065C70A09F3") :
                #datetime_object = datetime.strptime(self.dict['DTA_COMPARECIMENTO_PESSOA'], '%Y-%m-%dT%H:%M:%S.000Z')
                datetime_object = self.dict['DTA_COMPARECIMENTO_PESSOA']
                # print(datetime_object)
                somadatas = (datetime_object+timedelta(days=84))
                # print(somadatas)
                somadatas = somadatas.strftime("%Y-%m-%dT%H:%M:%S")
                #print('SOMADATAS ASTRAZENECA = '+somadatas)

                self.dict['DTA_APRAZAMENTO'] = str(somadatas+".000Z")
                #print("Data aprazamento parseado = "+ self.dict['DTA_APRAZAMENTO'])
                #print("data aprazamento "+somadatas)

            #print (self.dict['DTA_COMPARECIMENTO_PESSOA'])
            self.dict['DTA_COMPARECIMENTO_PESSOA'] = self.dict['DTA_COMPARECIMENTO_PESSOA'].strftime("%Y-%m-%dT%H:%M:%S.000Z")
            self.dict['DTA_COMPARECIMENTO_PESSOA'] = str(self.dict['DTA_COMPARECIMENTO_PESSOA'])

            #salva agenda parseada
            self.list_agenda_parsed.append(self.dict)

    def get_list_agenda_parsed_full(self):
        return self.list_agenda_parsed

    def get_list_agenda_parsed_line(self,line):
        return self.list_agenda_parsed[line]

    def size_list_agenda_parsed(self):
        return len(self.list_agenda_parsed)

#Cria e transforma objetos Cadastros em listas para poder enviar para o worker depois:
def CreateRegistersToSend():

    registers_to_send.clear()

    for i in range(len(list_to_send)):
        cadastros.append(i)
        cadastros[i]=RegisterBatch(list_to_send[i],table_index)
        cadastros[i].parse_to_dict()
        registers_to_send.append(cadastros[i].get_list_agenda_parsed_full())

# Funcoes referentes ao Message Actor que irá distribuir as mensagens (conteudo) entre os workers
@ray.remote
class MessageActor(object) :
    def __init__(self) :
        self.messages = []

    def add_message(self, message) :
        self.messages.append(message)

    def get_and_clear_messages(self) :
        messages = self.messages
        self.messages = []
        return messages


# Funcoes referentes ao Vacivida
@ray.remote
class Vacivida_Sys:
    def __init__(self):
        self.auth_message = "Nao autenticado"
        self.consult_message = "Sem mensagens"
        self.vacina_message = "Tomou ZERO doses"
        self.cadastro_message = "Cadastro nao iniciado"
        self.imunizar_message = "Imunizacao nao iniciada"

    #1. Realiza autenticacao no Vacivida
    def autenticar(self):

        self.headers = {
            'Connection': 'keep-alive',
            'access-control-allow-origin': 'http://portalvacivida.sp.gov.br/',
            'Accept': 'application/json, text/plain, */*',
            'sec-ch-ua-mobile': '?0',
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.96 Safari/537.36',
            'sec-ch-ua': '"Chromium";v="88", "Google Chrome";v="88", ";Not A Brand";v="99"',
            'Content-Type': 'application/json',
            'Origin': 'https://vacivida.sp.gov.br',
            'Sec-Fetch-Site': 'same-site',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Dest': 'empty',
            'Referer': 'https://vacivida.sp.gov.br/',
            'Accept-Language': 'pt,en-US;q=0.9,en;q=0.8',
        }
        self.data = login_vacivida
        response_login = requests.post('https://servico.vacivida.sp.gov.br/Usuario/Logar', headers=self.headers, data=self.data)

        #print(login_token)

        #transforma resposta em chaves
        self.login_token = json.loads(response_login.text)

        #faz leitura do token e salva como variavel
        self.login_token = self.login_token['Data']
        # print("Autenticado! Token armazenado na variavel login_token")
        self.auth_message="Autenticado!"


    def get_auth_message(self):
        return self.auth_message


    #2. Consulta CPF
    #@retry(stop_max_attempt_number=7)
    def consultacpf(self,cpf_paciente):
        self.CPFusuario = cpf_paciente
        self.headers = {
            'Connection': 'keep-alive',
            'TKP': '0',
            'AccessToken': self.login_token,
            'sec-ch-ua-mobile': '?0',
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.96 Safari/537.36',
            'Content-Type': 'application/json',
            'access-control-allow-origin': 'http://portalvacivida.sp.gov.br/',
            'Accept': 'application/json, text/plain, */*',
            'sec-ch-ua': '"Chromium";v="88", "Google Chrome";v="88", ";Not A Brand";v="99"',
            'Origin': 'https://vacivida.sp.gov.br',
            'Sec-Fetch-Site': 'same-site',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Dest': 'empty',
            'Referer': 'https://vacivida.sp.gov.br/',
            'Accept-Language': 'pt,en-US;q=0.9,en;q=0.8',
        }

        response_cpf = requests.get('https://servico.vacivida.sp.gov.br/Paciente/cnsoucpf/'+self.CPFusuario, headers=self.headers,timeout=100)
        time.sleep(2)
        self.dados_cadastro = json.loads(response_cpf.text)

        #printa json inteiro:
        #print(self.dados_cadastro)

        #print(self.dados_cadastro['Message'], "CPF é: ", self.CPFusuario)
        #print(dados_cadastro['Message'])
        if ("Consulta realizada com sucesso! OBS.: Nenhum dado localizado com os Parâmetros enviados." in self.dados_cadastro['Message']):
            #print("Usuário NÃO Cadastrado! CPF = ", self.CPFusuario)
            self.consult_message=("Usuário NÃO Cadastrado! CPF = "+ self.CPFusuario)


        elif ("Consulta realizada com sucesso!" in self.dados_cadastro['Message']):
            #print("Usuário Cadastrado! CPF = ", self.CPFusuario)
            #print('ID do Paciente = ' + self.dados_cadastro['Data']['IdPaciente'])
            #print('Nome do Paciente = ' + self.dados_cadastro['Data']['Nome'])
            #print('CPF = ' + self.dados_cadastro['Data']['CPF'])
            self.consult_message = ("Usuário Cadastrado! CPF = "+ self.CPFusuario)
            self.id_paciente= self.dados_cadastro['Data']['IdPaciente']

        else:
            #print ("Erro na consulta do CPF!", self.CPFusuario)
            self.consult_message = "Erro na consulta do CPF!"+ self.CPFusuario

    #3. Consulta vacinacao para verificar se ja foi aplicado alguma vacina
    def consultavacinacao(self,paciente):
        self.id_paciente= paciente['ID_PACIENTE']
        self.cpf_paciente= paciente['NUM_CPF']

        #CPFusuario=str(CPFusuario)

        self.headers = {
            'Connection': 'keep-alive',
            'TKP': '0',
            'AccessToken': self.login_token,
            'sec-ch-ua-mobile': '?0',
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.96 Safari/537.36',
            'Content-Type': 'application/json',
            'access-control-allow-origin': 'http://portalvacivida.sp.gov.br/',
            'Accept': 'application/json, text/plain, */*',
            'sec-ch-ua': '"Chromium";v="88", "Google Chrome";v="88", ";Not A Brand";v="99"',
            'Origin': 'https://vacivida.sp.gov.br',
            'Sec-Fetch-Site': 'same-site',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Dest': 'empty',
            'Referer': 'https://vacivida.sp.gov.br/',
            'Accept-Language': 'pt,en-US;q=0.9,en;q=0.8',
        }

        response_vacinacao = requests.get(
            'https://servico.vacivida.sp.gov.br/Vacinacao/Historico-Vacinacao-Paciente/'+self.id_paciente, headers=self.headers, timeout=500)
        #response_vacinacao = requests.get('https://servico.vacivida.sp.gov.br/Vacinacao/Historico-Vacinacao-Paciente/'+self.dados_cadastro['Data']['IdPaciente'], headers=self.headers, timeout=5)
        dados_vacinacao = json.loads(response_vacinacao.text)

        #3.1 Notifica quantas doses foram tomadas e cria flag "vacinado"
        if len(dados_vacinacao['Data']['vacinacao']) == 0:
            #print("CPF = ", self.cpf_paciente," Nao vacinado!")
            self.vacina_messsage="CPF = "+ self.cpf_paciente+" Tomou ZERO doses"
            vacinado=0
        elif len(dados_vacinacao['Data']['vacinacao']) == 1:
            #print("CPF = ", self.cpf_paciente," Tomou 1 dose")
            self.vacina_messsage ="CPF = "+ self.cpf_paciente + " Tomou UMA dose"
            vacinado=1
        elif len(dados_vacinacao['Data']['vacinacao']) == 2:
            #print("CPF = ", self.cpf_paciente," Tomou 2 doses")
            self.vacina_messsage = "CPF = " + self.cpf_paciente + " Tomou DUAS doses"
            vacinado=2
        #else:
            #print("CPF = ", self.cpf_paciente," Erro! Mais de 2 doses no registro")
        #print("CPF = ", self.cpf_paciente," Imunobiologico = " + dados_vacinacao['Data']['vacinacao'][0]['Imunobiologico'])


    def get_vacina_message(self):
        return self.vacina_message

    def get_id_paciente(self):
        return self.id_paciente

    def get_consult_message(self):
        return self.consult_message

    #4. Realiza cadastro do paciente
    def cadastrar_paciente(self,objpaciente):
        self.objpaciente= objpaciente
        self.headers = {
            'Connection': 'keep-alive',
            'TKP': '0',
            'AccessToken': self.login_token,
            'sec-ch-ua-mobile': '?0',
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.96 Safari/537.36',
            'Content-Type': 'application/json',
            'access-control-allow-origin': 'http://portalvacivida.sp.gov.br/',
            'Accept': 'application/json, text/plain, */*',
            'sec-ch-ua': '"Chromium";v="88", "Google Chrome";v="88", ";Not A Brand";v="99"',
            'Origin': 'https://vacivida.sp.gov.br',
            'Sec-Fetch-Site': 'same-site',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Dest': 'empty',
            'Referer': 'https://vacivida.sp.gov.br/',
            'Accept-Language': 'pt,en-US;q=0.9,en;q=0.8',
        }

        self.datacadastro = '{"Data":{"PesqCNS_CPF":"'+self.objpaciente['NUM_CPF']+'",' \
               '"CNS":null,' \
               '"CPF": "'+self.objpaciente['NUM_CPF']+'",' \
               '"Nome":"'+self.objpaciente['DSC_NOME']+'",' \
               '"NomeMae":"'+self.objpaciente['DSC_NOME_MAE']+'",' \
               '"NomeSocial":null,' \
               '"DataNascimento":"'+self.objpaciente['DTA_NASCIMENTO']+'",' \
               '"CodigoSexo":"'+self.objpaciente['TPO_SEXO']+'",' \
               '"IdRaca":"'+self.objpaciente['DSC_RACA_COR']+'",' \
               '"IdEtnia":null,' \
               '"NumeroTelefone":"'+self.objpaciente['NUM_TELEFONE']+'",' \
               '"Gestante":"'+self.objpaciente['GESTANTE']+'",' \
               '"Puerpera":"'+self.objpaciente['PUERPERA']+'",' \
               '"IdPaisResidencia":"'+self.objpaciente['PAIS']+'",' \
               '"IdUFResidencia":"'+self.objpaciente['UF']+'",' \
               '"IdMunicipioResidencia":"'+self.objpaciente['MUNICIPIO']+'",' \
               '"ZonaMoradia":"'+self.objpaciente['ZONA']+'",' \
               '"LogradouroResidencia":"'+self.objpaciente['DSC_ENDERECO']+'",' \
               '"NumeroLogradouroResidencia":"'+self.objpaciente['NUM_ENDERECO']+'",' \
               '"Bairro":"'+self.objpaciente['BAIRRO']+'",' \
               '"ComplementoLogradouroResidencia":null,' \
               '"Email":"'+self.objpaciente['DSC_EMAIL']+'",' \
               '"Estrangeiro":false,' \
               '"IdPaciente":"",' \
               '"Telefones":[{' \
               '"IdPacienteTelefone":"",' \
               '"IdPaciente":"",' \
               '"DDD":"'+self.objpaciente['NUM_TELEFONE_DDD']+'","Telefone":"'+self.objpaciente['NUM_TELEFONE_NUM']+'"}]},' \
               '"AccessToken":"'+self.login_token+'"}'

        time.sleep(5)
        #print("DEBUG - Data Cadastro = ", self.datacadastro)
        #requests.post
        response_incluir = requests.post('https://servico.vacivida.sp.gov.br/Paciente/incluir-paciente', headers=self.headers, data=self.datacadastro, timeout=500)
        time.sleep(5)
        #print("DEBUG - Response incluir= ", response_incluir)
        self.response_incluir=response_incluir
        self.dados_incluir=json.loads(response_incluir.text)
        #print("DEBUG - Dados incluir= ",self.dados_incluir)
        #print(self.dados_incluir)  #printa resposta recebida da solicitacao de cadastro

        datastring= json.dumps(self.datacadastro)


        if (self.dados_incluir['ValidationSummary'] != None) :
            # print(self.dados_incluir['ValidationSummary']['Erros'][0]['ErrorMessage'])
            self.cadastro_message = str(self.dados_incluir['ValidationSummary']['Erros'][0]['ErrorMessage'])+" CPF : "+\
                                    self.objpaciente['NUM_CPF']
        elif ("Incluído com Sucesso" in self.dados_incluir['Message']) :
            # print("Incluido com sucesso")
            self.cadastro_message = str(self.dados_incluir['Message'])+" CPF: "+self.objpaciente[
                'NUM_CPF']+" cadastrado "

    def get_cadastro_message(self) :
        return self.cadastro_message

    # 5. Realiza registro da imunizacao
    def imunizar(self, objimunizacao):
        self.objimunizacao=objimunizacao
        self.headers = {
            'Connection': 'keep-alive',
            'TKP': '0',
            'AccessToken': self.login_token,
            'sec-ch-ua-mobile': '?0',
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.96 Safari/537.36',
            'Content-Type': 'application/json',
            'access-control-allow-origin': 'http://portalvacivida.sp.gov.br/',
            'Accept': 'application/json, text/plain, */*',
            'sec-ch-ua': '"Chromium";v="88", "Google Chrome";v="88", ";Not A Brand";v="99"',
            'Origin': 'https://vacivida.sp.gov.br',
            'Sec-Fetch-Site': 'same-site',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Dest': 'empty',
            'Referer': 'https://vacivida.sp.gov.br/',
            'Accept-Language': 'pt,en-US;q=0.9,en;q=0.8',
        }
        #print('LOG! = ', self.objimunizacao)

        self.data_imunizar = '{"Data":{"IdGrupoAtendimento":"'+str(objimunizacao['DSC_AREA'])+'",' \
                             '"IdEstrategia":"'+objimunizacao['ESTRATEGIA']+'",' \
                             '"IdImunobiologico":"'+objimunizacao['DSC_TIPO_VACINA']+'",' \
                             '"IdDose":"'+objimunizacao['NUM_DOSE_VACINA']+'",' \
                             '"DataVacinacao":"'+objimunizacao['DTA_COMPARECIMENTO_PESSOA']+'",' \
                             '"DataAprazamento":"'+objimunizacao['DTA_APRAZAMENTO']+'",'\
                             '"IdLote":"'+objimunizacao['NUM_LOTE_VACINA']+'",' \
                             '"IdViaAdministracao":"'+objimunizacao['VIA_ADMINISTRACAO']+'",' \
                             '"IdLocalAplicacao":"'+objimunizacao['LOCAL_APLICACAO']+'",' \
                             '"IdVacinador":"'+objimunizacao['VACINADOR']+'",' \
                             '"IdPaciente":"'+objimunizacao['ID_PACIENTE']+'",' \
                             '"IdEstabelecimento":"'+objimunizacao['ESTABELECIMENTO']+'"},' \
                             '"AccessToken":"'+self.login_token+'"}'

        #global global_headers
        #global_headers = self.headers
        #global global_data_imunizar
        #global_data_imunizar = self.data_imunizar
        time.sleep(5)
        #print("DATA IMUNIZAR = "+ self.data_imunizar)
        response_imunizar = requests.post('https://servico.vacivida.sp.gov.br/Vacinacao/Inserir-Vacinacao', headers=self.headers, data=self.data_imunizar, timeout=500)
        time.sleep(5)
        #print(response_imunizar)
        self.response_imunizar=response_imunizar


        self.response_imunizar=json.loads(self.response_imunizar.text)


        if (self.response_imunizar['ValidationSummary'] != None) :
            # print(self.dados_incluir['ValidationSummary']['Erros'][0]['ErrorMessage'])
            self.imunizar_message = str(self.response_imunizar['ValidationSummary']['Erros'][0]['ErrorMessage'])+" CPF : "+\
                                    self.objimunizacao['NUM_CPF']
        elif ("Incluído com Sucesso" in self.response_imunizar['Message']) :
            # print("Incluido com sucesso")
            self.imunizar_message = str(self.response_imunizar['Message'])+" CPF: "+self.objimunizacao[
                'NUM_CPF']+" imunizado "


    def get_imunizar_message(self) :
        return self.imunizar_message



@ray.remote
def worker(message_actor, j, cadastros_to_send) :
    a = time.time()
    d = time.time()
    time.sleep(round(random.uniform(0, 2), 2))  # jitter aumenta tempo de espera em um numero randomico entre 0 e 1
    vacivida= Vacivida_Sys.remote()
    vacivida.autenticar.remote()
    #print (ray.get(vacivida.get_auth_message.remote()))
    auth_message=ray.get(vacivida.get_auth_message.remote())
    #message_actor.add_message.remote("Worker {} = {}.".format(j, auth_message ))
    print("Worker " , j , auth_message)
    cadastros_worker=cadastros_to_send[j]
    time.sleep(2)
    for i in cadastros_worker:
        d = time.time()

        # habilita loop de autenticacao a cada d-a segundos
        if (d-a>1800):
            time.sleep(round(random.uniform(0, 2), 2))  # jitter aumenta tempo de espera em um numero randomico entre 0 e 1
            d=time.time()
            a=time.time()
            print("Passaram 30min, fazendo nova autenticacao para o Worker ", j)
            vacivida.autenticar.remote()
            auth_message = ray.get(vacivida.get_auth_message.remote())
            print("Worker ", j, auth_message)
            cadastros_worker = cadastros_to_send[j]



        #print(i['SEQ_AGENDA'])
        time.sleep(1)
        seqtoprint=i['SEQ_AGENDA']
        cpftoprint=i['NUM_CPF']
        #message_actor.add_message.remote(
        #    "SEQ_AGENDA = {} , NUM_CPF = {} reading at worker {}.".format(seqtoprint, cpftoprint, j))


        vacivida.consultacpf.remote(i['NUM_CPF'])
        cadastro_status=ray.get(vacivida.get_consult_message.remote(), timeout=30)


        print (cadastro_status)

        if ("Usuário NÃO Cadastrado" in cadastro_status):
            con = cx_Oracle.connect(connection_params)
            cur = con.cursor()
            cur.execute(f"UPDATE age_agendamento_covid set ind_vacivida_cadastro ='F' where SEQ_AGENDA = {i['SEQ_AGENDA']} ")
            #print("DEBUG - Cadastro SEQ_AGENDA = ", i['SEQ_AGENDA'], " atualizado para False")

            cur.execute(f"UPDATE age_agendamento_covid set ind_vacivida_vacinacao ='F' where SEQ_AGENDA = {i['SEQ_AGENDA']} ")
            #print("DEBUG - Vacinacao SEQ_AGENDA = ", i['SEQ_AGENDA'], " atualizado para False")
            # for result in cur:
            # print(result)
            con.commit()
            con.close()

            #max retry default = 5
            for x in range(MAX_RETRY):
                time.sleep(3*x) #backoff = tentativas * multiplicacao do tmepo de espera
                time.sleep(round(random.uniform(0,1), 2)) #jitter aumenta tempo de espera em um numero randomico entre 0 e 1

                #print("DEBUG = Cadastrar paciente = ", i)
                vacivida.cadastrar_paciente.remote(i)
                cadastrar_status=ray.get(vacivida.get_cadastro_message.remote(), timeout=50)
                print( cadastrar_status)
                if ("cadastrado" in cadastrar_status):
                    con = cx_Oracle.connect(connection_params)
                    cur = con.cursor()
                    cur.execute(f"UPDATE age_agendamento_covid set ind_vacivida_cadastro ='T' where SEQ_AGENDA = {i['SEQ_AGENDA']} ")

                    con.commit()
                    con.close()
                    break
                # adicionar vacina a partir daqui dentro de um if
                elif ("Erro" in cadastrar_status):
                    pass



            #on.commit()
            #con.close()



            #update no bd que nao esta cadastrado
        elif ("Usuário Cadastrado") in cadastro_status:
            i['ID_PACIENTE'] = ray.get(vacivida.get_id_paciente.remote(), timeout=30)
            #print("DEBUG - ID_PACIENTE no vacivida= ", i['ID_PACIENTE'])


            #falta inserir coluna com ID usuario para cruzar no vacivida mais rapido

            con = cx_Oracle.connect(connection_params)
            cur = con.cursor()
            cur.execute(f"UPDATE age_agendamento_covid set ind_vacivida_cadastro ='T' where SEQ_AGENDA = {i['SEQ_AGENDA']} ")
            print("Cadastro SEQ_AGENDA = ", i['SEQ_AGENDA'], " atualizado para True")
            con.commit()
            con.close()
            time.sleep(2)

            # for result in cur:
            # print(result)


            #Cadastrar Imunizacao
            #comentar essa area para apenas atualizar o banco de dados
            for x in range(MAX_RETRY):
                time.sleep(2*x) #backoff = tentativas * multiplicacao do tmepo de espera
                time.sleep(round(random.uniform(0, 1), 2))  # jitter aumenta tempo de espera em um numero randomico entre 0 e 1
                #print("DEBUG - Object to imunizar = ", i)

                vacivida.imunizar.remote(i)
                imunizar_status=ray.get(vacivida.get_imunizar_message.remote(), timeout=30)
                print(imunizar_status)
                if ("Incluído com Sucesso" in imunizar_status):
                    con = cx_Oracle.connect(connection_params)
                    cur = con.cursor()
                    cur.execute(f"UPDATE age_agendamento_covid set ind_vacivida_vacinacao ='T' where SEQ_AGENDA = {i['SEQ_AGENDA']} ")
                    #for result in cur :
                    #    print(result)
                    print("Vacinacao SEQ_AGENDA = ", i['SEQ_AGENDA'], " atualizado para True")
                    con.commit()
                    con.close()
                    break
                elif ("já tomou esta dose" in imunizar_status):
                    con = cx_Oracle.connect(connection_params)
                    cur = con.cursor()
                    cur.execute(f"UPDATE age_agendamento_covid set ind_vacivida_vacinacao ='T' where SEQ_AGENDA = {i['SEQ_AGENDA']} ")
                    #for result in cur :
                    #    print(result)
                    print("Vacinacao SEQ_AGENDA = ", i['SEQ_AGENDA'], " atualizado para True")
                    con.commit()
                    con.close()
                    break

                #para o caso de ja ter sido cadastrado um tipo de vacina diferente, registra no BD como "E" (erro)
                elif ("Não é permitido que a 2ª dose da vacina seja diferente da 1ª dose CPF" in imunizar_status):
                    con = cx_Oracle.connect(connection_params)
                    cur = con.cursor()
                    cur.execute(
                        f"UPDATE age_agendamento_covid set ind_vacivida_vacinacao ='E' where SEQ_AGENDA = {i['SEQ_AGENDA']} ")
                    # for result in cur :
                    #    print(result)
                    print("Vacinacao SEQ_AGENDA = ", i['SEQ_AGENDA'], " atualizado para Erro")
                    con.commit()
                    con.close()
                    break

                elif ("A primeira dose do paciente não está registrado no VaciVida." in imunizar_status):
                    con = cx_Oracle.connect(connection_params)
                    cur = con.cursor()
                    cur.execute(
                        f"UPDATE age_agendamento_covid set ind_vacivida_vacinacao ='I' where SEQ_AGENDA = {i['SEQ_AGENDA']} ")
                    # for result in cur :
                    #    print(result)
                    print("Vacinacao SEQ_AGENDA = ", i['SEQ_AGENDA'], " atualizado para Inconsistente")
                    con.commit()
                    con.close()
                    break

                elif ("Não é permitido vacinar paciente menor de 18 anos de idade" in imunizar_status):
                    con = cx_Oracle.connect(connection_params)
                    cur = con.cursor()
                    cur.execute(
                        f"UPDATE age_agendamento_covid set ind_vacivida_vacinacao ='X' where SEQ_AGENDA = {i['SEQ_AGENDA']} ")
                    # for result in cur :
                    #    print(result)
                    print("Vacinacao SEQ_AGENDA = ", i['SEQ_AGENDA'], " atualizado para Data de Nascimento Incorreto")
                    con.commit()
                    con.close()
                    break

                elif ("Erro" in imunizar_status):
                    pass

            '''
            # Verificar Banco de dados
            # Para apenas verificar as vacinas e atualizar o BD, comente a parte de Cadastrar Imunizacao e remova
            # os comentários abaixo.
            # Comentei aqui pois estava dando conflito para atualizar o BD junto com o Cadastrar Imunizacao
            # 
            
            vacivida.consultavacinacao.remote(i)
            vacina_status=ray.get(vacivida.get_vacina_message.remote(), timeout=30)
            print(vacina_status)
            if ( "ZERO" in vacina_status):
                con = cx_Oracle.connect(connection_params)
                cur = con.cursor()
                cur.execute(f"UPDATE age_agendamento_covid set ind_vacivida_vacinacao ='F' where SEQ_AGENDA = {i['SEQ_AGENDA']} ")
                print("Vacinacao SEQ_AGENDA = ", i['SEQ_AGENDA'], " atualizado para False")
                con.commit()
                con.close()

            elif ("UMA" in vacina_status) and ("B66C4B622F1F840AE053D065C70A17A1" in i['NUM_DOSE_VACINA']):
                con = cx_Oracle.connect(connection_params)
                cur = con.cursor()
                cur.execute(f"UPDATE age_agendamento_covid set ind_vacivida_vacinacao ='T' where SEQ_AGENDA = {i['SEQ_AGENDA']} ")
                print("Vacinacao SEQ_AGENDA = ", i['SEQ_AGENDA'], " atualizado para True")
                con.commit()
                con.close()

            elif ("DUAS" in vacina_status) and ("B66C4B622F21840AE053D065C70A17A1" in i['NUM_DOSE_VACINA']):
                con = cx_Oracle.connect(connection_params)
                cur = con.cursor()
                cur.execute(f"UPDATE age_agendamento_covid set ind_vacivida_vacinacao ='T' where SEQ_AGENDA = {i['SEQ_AGENDA']} ")
                print("Vacinacao SEQ_AGENDA = ", i['SEQ_AGENDA'], " atualizado para True")
                con.commit()
                con.close()

            else:
                #cur.execute(f"UPDATE age_agendamento_covid set ind_vacivida_vacinacao ='F' where SEQ_AGENDA = {i['SEQ_AGENDA']} ")
                #print("Vacinacao SEQ_AGENDA = ", i['SEQ_AGENDA'], " atualizado para False")
                print("Erro na vacinacao SEQ_AGENDA = ", i['SEQ_AGENDA'], " verificar log")
            '''

        #cadastro_status=ray.get(vacivida.get_consult_message)
        #print(cadastro_status)
        #message_actor.add_message.remote("Worker {} = CPF {} = {} .".format(j, cpftoprint, cadastro_status))
        #message_actor.get_DB_line.remote(i)


# Create a message actor.
message_actor = MessageActor.remote()

# Start 3 tasks that push messages to the actor.
#[worker.remote(message_actor, j, registers_to_send) for j in range(n_workers)]

# Loop para reiniciar os registros a cada time.sleep(x) tempo
# Dica: em momentos de instabilidade, pode ser interessante reduzir o tempo para apenas alguns minutos
while True:
    print ("Inicializando processos...")
    GetDB()
    CreateRegistersToSend()
    [worker.remote(message_actor, j, registers_to_send) for j in range(n_workers)]
    new_messages = ray.get(message_actor.get_and_clear_messages.remote())
    print("New messages:", new_messages)
    time.sleep(6000) #reinicia processos a cada X segundos