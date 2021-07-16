'''
VACIBOT v.08 - Bot para automatizacao de registros no Vacivida para o COVID19
by Victor Fragoso (https://github.com/victorffs), Willian Sanches (https://github.com/wi-sanc)- Prefeitura Municipal de Santo André
Date: 09-jul-2021
Version: v.08
License: MIT License

'''

# Importacao de bibliotecas Python:
import ray
import time
import numpy as np
import json
import requests
from unidecode import unidecode
from datetime import datetime, timedelta
import cx_Oracle
import random
import re
import sys

# Importacao de arquivos de configuracao:
from credentials import connection_params
from credentials import login_vacivida
import dicts as di

# DEFINES
n_workers = 2  # recomendado nao passar de 40 workers por credentials.py
MAX_RETRY = 5  # attempts
standalonemode = True
headnodeip = '127.0.0.1:6379'
headnodepassword = '5241590000000000'

#argument for n_workers
try:
    n_workers = int(sys.argv[1])
except:
    pass

# Global variables
table_index = []            #Nomes das colunas da select_query
global lotes
lotes = {}


# Precisa adicionar na funcao parse_to_dict() as keys caso sejam adicionadas novas
# SELECT:
select_query = "SELECT * FROM VACIBOT_SELECT"


# OPCAO PARA STANDALONE OU DISTRIBUIDO
if standalonemode :
    ray.init()
else :
    ray.init(address=headnodeip, _redis_password=headnodepassword)


# inicializa listas vazias
def GetDB(filter_area=None) :
    # limpa as listas
    list_to_send = []
    list_seq_agenda = []

    ####query que salva e envia TODAS as colunas
    con = cx_Oracle.connect(connection_params)
    cur = con.cursor()
    cur.execute(f'{select_query}' + (f" where DSC_AREA = '{filter_area}'" if filter_area else "") )
    # registra indexes
    for i in cur.description :
        # print(i[0])
        table_index.append(i[0])
    for result in cur :
        list_seq_agenda.append(result)
        pass
    con.close()

    lista_dividida = np.array_split(list_seq_agenda, n_workers)  # divide a lista de seqs pela quantidade de workers

    # Coloca o resultado do split em list_to_send de acordo com a qntde de workers
    for array in lista_dividida :
        # print (array)
        list_to_send.append(array.tolist())

    # tamanho da lista sera utilizado para definir o tamanho do loop dos workers
    size_of_list = len(list_seq_agenda) / n_workers

    print(f"########## [{area}] Tamanho da lista: ", len(list_seq_agenda))

    return list_to_send, list_seq_agenda


# Cria classe RegisterBatch para poder realizar operacoes em cima de forma mais facil
class RegisterBatch() :
    def __init__(self, list_agenda, list_index) :
        self.list_agenda = list_agenda      #sublista de list_to_send (ie, lista de registros para um worker)
        self.list_index = list_index        #table_index (cabecalho das colunas)

    def parse_to_dict(self, ) :
        self.list_agenda_parsed = []
        for list_agenda_line in self.list_agenda :
            self.dict = {self.list_index[0] : str(list_agenda_line[0]),  # SEQ_AGENDA
                         self.list_index[1] : str(list_agenda_line[1]),  # new: DSC_PUBLICO // old: 'DSC_AREA'
                         self.list_index[2] : str(list_agenda_line[2]),  # 'DSC_NOME'
                         self.list_index[3] : str(list_agenda_line[3]),  # 'NUM_CPF'
                         self.list_index[4] : str(list_agenda_line[4]),  # 'DSC_NOME_MAE'
                         self.list_index[5] : str(list_agenda_line[5]),  # 'NUM_CNS'
                         self.list_index[6] : list_agenda_line[6],  # 'DTA_NASCIMENTO'
                         self.list_index[7] : str(list_agenda_line[7]),  # 'NUM_TELEFONE'
                         self.list_index[8] : str(list_agenda_line[8]),  # 'DSC_EMAIL'
                         self.list_index[9] : str(list_agenda_line[9]),  # 'DSC_ENDERECO'
                         self.list_index[10] : str(list_agenda_line[10]),  # 'NUM_CEP'
                         self.list_index[11] : str(list_agenda_line[11]),  # 'TPO_SEXO'
                         self.list_index[12] : str(list_agenda_line[12]),  # 'DSC_RACA_COR'
                         self.list_index[13] : str(list_agenda_line[13]),  # 'DSC_ORGAO_CLASSE'
                         self.list_index[14] : str(list_agenda_line[14]),  # 'NUM_REGISTRO_CLASSE'
                         self.list_index[15] : list_agenda_line[15],  # 'DTA_COMPARECIMENTO_PESSOA'
                         self.list_index[16] : str((list_agenda_line[16])),  # 'NUM_LOTE_VACINA'
                         self.list_index[17] : str(list_agenda_line[17]),  # 'DSC_TIPO_VACINA'
                         self.list_index[18] : str(list_agenda_line[18]),  # 'NUM_DOSE_VACINA'
                         self.list_index[19] : str(list_agenda_line[19]),  # 'DSC_OBSERVACAO'
                         self.list_index[20] : str(list_agenda_line[20]),  # 'NUM_BPC'
                         self.list_index[21] : str(list_agenda_line[21]),  # 'NUM_CRM'
                         self.list_index[22] : str(list_agenda_line[22]),  # 'DSC_COMORBIDADES'
                         self.list_index[23] : str(list_agenda_line[23]),  # 'IND_VACIVIDA_CADASTRO'
                         self.list_index[24] : str(list_agenda_line[24]),  # 'IND_VACIVIDA_VACINACAO'
                         self.list_index[25] : str(list_agenda_line[25])   # 'DSC_AREA'
                         }

            # OTHERS KEYS:
            # GESTANTE
            # PUERPERA
            # TELDDD
            # TELNUM

            # parse CPF
            # adiciona zero a esquerda quando CPF tiver 10 digitos
            if (len(self.dict['NUM_CPF']) == 11) :
                # print ("DEBUG - CPF possui 11 digitos. CPF = " + self.dict['NUM_CPF'])
                pass
            elif (len(self.dict['NUM_CPF']) == 10) :
                self.dict['NUM_CPF'] = self.dict['NUM_CPF'][:0]+'0'+self.dict['NUM_CPF'][0 :]
            elif (len(self.dict['NUM_CPF']) == 9) :
                self.dict['NUM_CPF'] = self.dict['NUM_CPF'][:0]+'00'+self.dict['NUM_CPF'][0 :]
            elif (len(self.dict['NUM_CPF']) == 8) :
                self.dict['NUM_CPF'] = self.dict['NUM_CPF'][:0]+'000'+self.dict['NUM_CPF'][0 :]
            elif (len(self.dict['NUM_CPF']) == 7) :
                self.dict['NUM_CPF'] = self.dict['NUM_CPF'][:0]+'0000'+self.dict['NUM_CPF'][0 :]
            else :
                print("CPF invalido! CPF = "+self.dict['NUM_CPF'])

            # parse nascimento
            self.dict['DTA_NASCIMENTO'] = self.dict['DTA_NASCIMENTO'].strftime("%Y-%m-%dT%H:%M:%S.000Z")

            # necessario fazer normalizacao dos caracteres removendo acentuacao com unidecode
            self.dict['DSC_NOME'] = unidecode(self.dict['DSC_NOME']).upper()
            self.dict['DSC_NOME_MAE'] = unidecode(self.dict['DSC_NOME_MAE']).upper()
            self.dict['DSC_ENDERECO'] = unidecode(self.dict['DSC_ENDERECO']).upper()
            self.dict['DSC_EMAIL'] = unidecode(self.dict['DSC_EMAIL']).upper()

            if (self.dict['TPO_SEXO'] == "None"):
                self.dict['TPO_SEXO'] = "I"  # usar sexo indefinido quando null no db
            if self.dict['TPO_SEXO'] not in ["M","F","I"]:
                print(f"Sexo inválido:{self.dict['TPO_SEXO']} ")

            # Consulta raca por tags
            if   (self.dict['DSC_RACA_COR'] in ["Amarela", "AMARELA", "amarela", "Amarelo", "AMARELO", "amarelo"] ) :
                self.dict['DSC_RACA_COR'] = "B66C4B622EF3840AE053D065C70A17A1"
            elif (self.dict['DSC_RACA_COR'] in ["Branca", "BRANCA", "branca", "Branco", "BRANCO", "branco"] ) :
                self.dict['DSC_RACA_COR'] = "B66C4B622EF4840AE053D065C70A17A1"
            elif (self.dict['DSC_RACA_COR'] in ["Indigena", "INDIGENA", "Indígena", "INDÍGENA", "indigena"] ) :
                self.dict['DSC_RACA_COR'] = "B66C4B622EF7840AE053D065C70A17A1"
            elif (self.dict['DSC_RACA_COR'] in ["Não informado", "Não Informada", "NÃO INFORMADA", "não informada", "nao informada"] ) :
                self.dict['DSC_RACA_COR'] = "B66C4B622EF8840AE053D065C70A17A1"
            elif (self.dict['DSC_RACA_COR'] in ["Preta", "Negra", "NEGRA", "negra", "Negro", "NEGRO", "negro"] ) :
                self.dict['DSC_RACA_COR'] = "B66C4B622EF5840AE053D065C70A17A1"
            elif (self.dict['DSC_RACA_COR'] in ["Parda", "PARDA", "parda", "Pardo", "PARDO", "pardo"] ) :
                self.dict['DSC_RACA_COR'] = "B66C4B622EF6840AE053D065C70A17A1"
            else:
                print(f"Raça/cor não identificado: {self.dict['DSC_RACA_COR']}")
                self.dict['DSC_RACA_COR'] = "B66C4B622EF8840AE053D065C70A17A1"  #Atribui valor de 'não informado'


            # adiciona 0 ao numero de telefone se for fixo para passar na validacao de 9 digitos
            if (len(self.dict['NUM_TELEFONE']) == 10) :
                self.dict['NUM_TELEFONE'] = self.dict['NUM_TELEFONE'][:2]+'0'+self.dict['NUM_TELEFONE'][2 :]
            if (len(self.dict['NUM_TELEFONE']) > 11) :
                self.dict['NUM_TELEFONE'] = self.dict['NUM_TELEFONE'][(len(self.dict['NUM_TELEFONE'])-11) :]

            if (len(self.dict['NUM_TELEFONE']) < 8) :
                self.dict['NUM_TELEFONE'] = "11123456789"

            self.dict['NUM_TELEFONE_DDD'] = self.dict['NUM_TELEFONE'][0 :2]
            self.dict['NUM_TELEFONE_NUM'] = self.dict['NUM_TELEFONE'][2 :len(self.dict['NUM_TELEFONE'])]

            # fixed values
            self.dict['GESTANTE'] = False
            self.dict['PUERPERA'] = False
            self.dict['PAIS'] = "B66C4B622E1B840AE053D065C70A17A1"
            self.dict['UF'] = "B66C4B622DF6840AE053D065C70A17A1"
            self.dict['MUNICIPIO'] = "B66C4B623E5E840AE053D065C70A17A1"
            self.dict['ZONA'] = "U"
            self.dict['NUM_ENDERECO'] = "1"  # precisa criar parser
            self.dict['BAIRRO'] = "bairro"  # precisa criar parser
            self.dict['COMPLEMENTO'] = "complemento"
            self.dict['1A_DOSE_OUTRO_ESTADO'] = "null"
            self.dict['1A_DOSE_OUTRO_PAIS'] = "null"

            if (self.dict['DSC_OBSERVACAO'] != 'None'):
                unidecode(self.dict['DSC_OBSERVACAO']).upper()
                if ("1A DOSE EM OUTRO ESTADO" in self.dict['DSC_OBSERVACAO']):
                    pass


            if (self.dict['NUM_CRM'] == 'None'):
                self.dict['NUM_CRM'] = "40787"
            else:
                self.dict['NUM_CRM'] = re.sub(r'\D','', self.dict['NUM_CRM'] )   #Formata p/ somente numeros

            #### INFOS PARA VACINACAO
            # fixos:
            self.dict['ESTRATEGIA'] = "B66C4B622F6E840AE053D065C70A17A1"
            self.dict['VIA_ADMINISTRACAO'] = "B66C4B622F6B840AE053D065C70A17A1"
            self.dict['LOCAL_APLICACAO'] = "B66C4B622F25840AE053D065C70A17A1"
            self.dict['DSC_COMORBIDADES'] = "null"

            # print(self.dict['DSC_AREA'])

            #Local
            self.dict['VACINADOR'] = di.vacinador[di.area_alias[self.dict['DSC_AREA']]]
            self.dict['ESTABELECIMENTO'] = di.estabelecimento[di.area_alias[self.dict['DSC_AREA']]]

            # verifica gestante/puerpera
            if ("GESTANTE" in self.dict['DSC_COMORBIDADES']) :
                self.dict['GESTANTE'] = True
                self.dict['DSC_COMORBIDADES'] = "C1BDD007AB971C7DE053D065C70A7835"
                self.dict['TPO_SEXO'] = "F"
            elif ("PUÉRPERA" in self.dict['DSC_COMORBIDADES']) :
                self.dict['PUERPERA'] = True
                self.dict['TPO_SEXO'] = "F"


            self.comorblist = []
            self.comorbstring = ''
            self.comorbdict = ''


            # parse grupo
            if   ("IDOSOS" in self.dict['DSC_PUBLICO']) :
                self.dict['DSC_PUBLICO'] = "BA0E494756847EBFE053D065C70AE389"
            elif ("SAÚDE" in self.dict['DSC_PUBLICO']) :
                self.dict['DSC_PUBLICO'] = "B83C80018F62B8B5E053D065C70AB1BB"
            elif ("EDUCAÇÃO" in self.dict['DSC_PUBLICO']) :
                self.dict['DSC_PUBLICO'] = "BF67A4F62608FD42E053D065C70A7D70"
            elif ("SEGURANÇA" in self.dict['DSC_PUBLICO']) :
                self.dict['DSC_PUBLICO'] = "BEC82DAA494479CEE053D065C70A2277"
            elif ("DEFICIÊNCIA" in self.dict['DSC_PUBLICO']) :
                self.dict['DSC_PUBLICO'] = "C1AB0FA7CA550BEDE053D065C70ADE01"
            elif ("COMORBIDADES" in self.dict['DSC_PUBLICO']) :
                self.dict['DSC_PUBLICO'] = "C1AB0FA7CA540BEDE053D065C70ADE01"
                
                #### comorbidade parser
                self.dict['DSC_COMORBIDADES'] = unidecode(self.dict['DSC_COMORBIDADES']).upper()
                #if ("GESTANTE" in self.dict['DSC_COMORBIDADES'] ):
                #    self.comorblist.append("C1BDD007AB971C7DE053D065C70A7835")
                #if ("PUERPERA" in self.dict['DSC_COMORBIDADES'] ):
                #    self.comorblist.append("C1BDD007AB981C7DE053D065C70A7835")
                if ("CIRROSE" in self.dict['DSC_COMORBIDADES'] ):
                    self.comorblist.append("C1ABF1B29F0AE2A1E053D065C70A072C")
                if ("DIABETES" in self.dict['DSC_COMORBIDADES'] ):
                    self.comorblist.append("C1ABF1B29F0BE2A1E053D065C70A072C")
                if ("DOENCA RENAL CRONICA" in self.dict['DSC_COMORBIDADES'] ):
                    self.comorblist.append("C1ABF1B29F0CE2A1E053D065C70A072C")
                if ("DOENCAS CARDIOVASCULARES" in self.dict['DSC_COMORBIDADES'] ):
                    self.comorblist.append("C1ABF1B29F1AE2A1E053D065C70A072C")
                if ("HEMOGLOBINOPATIA" in self.dict['DSC_COMORBIDADES'] ):
                    self.comorblist.append("C1ABF1B29F0EE2A1E053D065C70A072C")
                if ("HIPERTENSAO" in self.dict['DSC_COMORBIDADES'] ):
                    self.comorblist.append("C1ABF1B29F0FE2A1E053D065C70A072C")
                if ("OBESIDADE GRAVE" in self.dict['DSC_COMORBIDADES'] ):
                    self.comorblist.append("C1ABF1B29F12E2A1E053D065C70A072C")
                if ("PACIENTE ONCOLOGICO" in self.dict['DSC_COMORBIDADES'] ):
                    self.comorblist.append("C1ABF1B29F13E2A1E053D065C70A072C")
                if ("HIV" in self.dict['DSC_COMORBIDADES'] ):
                    self.comorblist.append("C1ABF1B29F14E2A1E053D065C70A072C")
                if ("PNEUMOPATIA" in self.dict['DSC_COMORBIDADES'] ):
                    self.comorblist.append("C1ABF1B29F15E2A1E053D065C70A072C")
                if ("SINDROME DE DOWN" in self.dict['DSC_COMORBIDADES'] ):
                    self.comorblist.append("C1ABF1B29F16E2A1E053D065C70A072C")
                if ("TRANSPLANTADO" in self.dict['DSC_COMORBIDADES'] ):
                    self.comorblist.append("C1ABF1B29F1BE2A1E053D065C70A072C")
                if ("IMUNOSSUPRIMIDO" in self.dict['DSC_COMORBIDADES'] ):
                    self.comorblist.append("C1ABF1B29F10E2A1E053D065C70A072C")
                if ("TERAPIA RENAL" in self.dict['DSC_COMORBIDADES'] ):
                    self.comorblist.append("C1ABF1B29F17E2A1E053D065C70A072C")

                if (self.dict['DSC_COMORBIDADES'] != "None" or len(self.comorblist)==0 ):
                    if (len(self.comorblist) == 1) :
                        self.comorbstring = '"'+self.comorblist[0]+'"'
                        self.comorbdict = '{"IdComorbidade":"'+self.comorblist[0]+'"}'
                        # so copia o valor do dicionario
                        pass

                    if (len(self.comorblist) >= 2) :
                        i = 0
                        for id in (self.comorblist) :
                            self.comorbstring = self.comorbstring+'"'+id+'"'
                            self.comorbdict = self.comorbdict+'{"IdComorbidade":"'+id+'"}'
                            i = i+1
                            if (len(self.comorblist)-i > 0) :
                                self.comorbstring = self.comorbstring+','
                                self.comorbdict = self.comorbdict+','

            elif ("SÍNDROME DE DOWN" in self.dict['DSC_PUBLICO']) :
                self.dict['DSC_PUBLICO'] = "C1AB0FA7CA540BEDE053D065C70ADE01"  # mesmo que comorbidade
                self.dict['DSC_COMORBIDADES'] = "C1ABF1B29F16E2A1E053D065C70A072C"
                self.comorbstring = '"C1ABF1B29F16E2A1E053D065C70A072C"'
                self.comorbdict = '{"IdComorbidade":"C1ABF1B29F16E2A1E053D065C70A072C"}'
            elif ("HEMODIÁLISE" in self.dict['DSC_PUBLICO']) :
                # hemodialise -> doenca renal cronica
                self.dict['DSC_PUBLICO'] = "C1AB0FA7CA540BEDE053D065C70ADE01"  # mesmo que comorbidade
                self.dict['DSC_COMORBIDADES'] = "C1ABF1B29F0CE2A1E053D065C70A072C"
                self.comorbstring = '"C1ABF1B29F0CE2A1E053D065C70A072C"'
                self.comorbdict = '{"IdComorbidade":"C1ABF1B29F0CE2A1E053D065C70A072C"}'
            elif ("IMUNOSSUPRESSOR" in self.dict['DSC_PUBLICO']) :
                self.dict['DSC_PUBLICO'] = "C1AB0FA7CA540BEDE053D065C70ADE01"  # mesmo que comorbidade
                self.dict['DSC_COMORBIDADES'] = "C1ABF1B29F10E2A1E053D065C70A072C"
                self.comorbstring = '"C1ABF1B29F10E2A1E053D065C70A072C"'
                self.comorbdict = '{"IdComorbidade":"C1ABF1B29F10E2A1E053D065C70A072C"}'
            elif ("MOTORISTAS E COBRADORES" in self.dict['DSC_PUBLICO']) :
                self.dict['DSC_PUBLICO'] = "C2379A994B453C7BE053D065C70AB01D"
            elif ("GESTANTES, PUÉRPERAS E LACTANTES" in self.dict['DSC_PUBLICO']) :    
                self.dict['DSC_PUBLICO'] = "C3CA9D0EC1B77C5BE053D065C70A77DC"   #população geral
                if( not ( ("GESTANTE" in self.dict['DSC_COMORBIDADES']) or ("PUÉRPERA" in self.dict['DSC_COMORBIDADES']) or ("LACTANTE" in self.dict['DSC_COMORBIDADES'])) ): #se não foi coletado a distinção, atribuir 'gestante'
                    self.dict['GESTANTE'] = True
                    self.dict['DSC_COMORBIDADES'] = "C1BDD007AB971C7DE053D065C70A7835"
                    self.dict['TPO_SEXO'] = "F"
            elif ("PESSOAS ENTRE" in self.dict['DSC_PUBLICO']):
                self.dict['DSC_PUBLICO'] = "C3CA9D0EC1B77C5BE053D065C70A77DC"
            elif ("AGENTES DE TRÂNSITO E DEFESA CIVIL" in self.dict['DSC_PUBLICO']):
                self.dict['DSC_PUBLICO'] = "C3CA9D0EC1B77C5BE053D065C70A77DC"
            elif ("PROFISSIONAIS DE LIMPEZA URBANA" in self.dict['DSC_PUBLICO']):
                self.dict['DSC_PUBLICO'] = "C3CA9D0EC1B77C5BE053D065C70A77DC"
            elif ("MOTORISTAS E AUXILIARES DE TRANSPORTE ESCOLAR" in self.dict['DSC_PUBLICO']):
                self.dict['DSC_PUBLICO'] = "C3CA9D0EC1B77C5BE053D065C70A77DC"
            elif ("PÚBLICO GERAL" in self.dict['DSC_PUBLICO']):
                self.dict['DSC_PUBLICO'] = "C3CA9D0EC1B77C5BE053D065C70A77DC"
            else :
                print("Grupo de vacinacao nao identificado!", self.dict['DSC_PUBLICO'])          

            self.dict['COMORBSTRING'] = self.comorbstring
            self.dict['COMORBDICT'] = self.comorbdict

            # parse dose
            if ('1' in self.dict['NUM_DOSE_VACINA']) :
                self.dict['NUM_DOSE_VACINA'] = "B66C4B622F1F840AE053D065C70A17A1"
            elif ('2' in self.dict['NUM_DOSE_VACINA']) :
                self.dict['NUM_DOSE_VACINA'] = "B66C4B622F21840AE053D065C70A17A1"
            if ("JANSSEN" in self.dict['DSC_TIPO_VACINA'] or "Janssen" in self.dict['DSC_TIPO_VACINA']):
                self.dict['NUM_DOSE_VACINA'] = "B8A51B8A4CF6F4E9E053D065C70A556D"       #dose única


            # parse tipo de vacina e lote
            found = False
            for key in di.vacina_id:
                if self.dict['DSC_TIPO_VACINA'] in key or key in self.dict['DSC_TIPO_VACINA']:  #key = fabricante da vacina
                    lote = self.dict['NUM_LOTE_VACINA']
                    self.dict['DSC_TIPO_VACINA'] = di.vacina_id[key]
                    self.dict['NUM_LOTE_VACINA'] = lotes[key].get(lote)
                    found = True

                    if self.dict['NUM_LOTE_VACINA'] == None:
                        print( f"Lote não identificado para {key}: {lote}" )
                        
                        #salva lotes c/ erro p/ consulta
                        with open("lotes_erro.csv", "a") as fp:
                            fp.write(f"{key},{lote},{datetime.now()}\n")
                    
                    break
            if not found:    
                print("Vacina não identificada: ", self.dict['DSC_TIPO_VACINA'])
            del found


            # calcula aprazamento e parser datas
            # print ("DEBUG - Data comparecimento = ", self.dict['DTA_COMPARECIMENTO_PESSOA'])

            # coronavac
            if (self.dict['DSC_TIPO_VACINA'] == "b309a279-f8b8-4023-b393-ed32723127ea") :
                # datetime_object = datetime.strptime(self.dict['DTA_COMPARECIMENTO_PESSOA'], '%Y-%m-%dT%H:%M:%S.000Z')
                datetime_object = self.dict['DTA_COMPARECIMENTO_PESSOA']
                # print(datetime_object)
                somadatas = (datetime_object+timedelta(days=28))
                # print(somadatas)
                somadatas = somadatas.strftime("%Y-%m-%dT%H:%M:%S")
                # print ('SOMADATAS CORONAVAC = ' + somadatas)

                self.dict['DTA_APRAZAMENTO'] = str(somadatas+".000Z")
                # print ("Data aprazamento parseado = " + self.dict['DTA_APRAZAMENTO'])
                # print("data aprazamento "+somadatas)

            # astrazeneca ou pfizer
            elif (self.dict['DSC_TIPO_VACINA'] in ["B9BAB192F78A60DBE053D065C70A09F3", "7694aac0-dedc-4d79-8639-61962efdad08"]) :
                # datetime_object = datetime.strptime(self.dict['DTA_COMPARECIMENTO_PESSOA'], '%Y-%m-%dT%H:%M:%S.000Z')
                datetime_object = self.dict['DTA_COMPARECIMENTO_PESSOA']
                # print(datetime_object)
                somadatas = (datetime_object+timedelta(days=84))
                # print(somadatas)
                somadatas = somadatas.strftime("%Y-%m-%dT%H:%M:%S")
                # print('SOMADATAS ASTRAZENECA = '+somadatas)

                self.dict['DTA_APRAZAMENTO'] = str(somadatas+".000Z")
                # print("Data aprazamento parseado = "+ self.dict['DTA_APRAZAMENTO'])
                # print("data aprazamento "+somadatas)

            # janssen
            else:
                self.dict['DTA_APRAZAMENTO'] = None

            # print (self.dict['DTA_COMPARECIMENTO_PESSOA'])
            self.dict['DTA_COMPARECIMENTO_PESSOA'] = self.dict['DTA_COMPARECIMENTO_PESSOA'].strftime(
                "%Y-%m-%dT%H:%M:%S.000Z")
            self.dict['DTA_COMPARECIMENTO_PESSOA'] = str(self.dict['DTA_COMPARECIMENTO_PESSOA'])

            # salva agenda parseada
            self.list_agenda_parsed.append(self.dict)

    def get_list_agenda_parsed_full(self) :
        return self.list_agenda_parsed

    def get_list_agenda_parsed_line(self, line) :
        return self.list_agenda_parsed[line]

    def size_list_agenda_parsed(self) :
        return len(self.list_agenda_parsed)


# Cria e transforma objetos Cadastros em listas para poder enviar para o worker depois:
def CreateRegistersToSend(list_to_send) :
    registers_to_send = []
    cadastros = []

    for i in range(len(list_to_send)) :
        cadastros.append(i)
        cadastros[i] = RegisterBatch(list_to_send[i], table_index)
        cadastros[i].parse_to_dict()
        registers_to_send.append(cadastros[i].get_list_agenda_parsed_full())

    return registers_to_send

def parse_paciente_json(objpaciente, id_paciente=""):
    paciente_json = {
        "PesqCNS_CPF":objpaciente['NUM_CPF'],
        "CNS":None,
        "CPF": objpaciente['NUM_CPF'],
        "Nome":objpaciente['DSC_NOME'],
        "NomeMae":objpaciente['DSC_NOME_MAE'],
        "NomeSocial":None,
        "DataNascimento":objpaciente['DTA_NASCIMENTO'],
        "CodigoSexo":objpaciente['TPO_SEXO'],
        "IdRaca":objpaciente['DSC_RACA_COR'],
        "IdEtnia":None,
        "NumeroTelefone":objpaciente['NUM_TELEFONE'],
        "Gestante":objpaciente['GESTANTE'],
        "Puerpera":objpaciente['PUERPERA'],
        "IdPaisResidencia":objpaciente['PAIS'],
        "IdUFResidencia":objpaciente['UF'],
        "IdMunicipioResidencia":objpaciente['MUNICIPIO'],
        "ZonaMoradia":objpaciente['ZONA'],
        "LogradouroResidencia":objpaciente['DSC_ENDERECO'],
        "NumeroLogradouroResidencia":objpaciente['NUM_ENDERECO'],
        "Bairro":objpaciente['BAIRRO'],
        "ComplementoLogradouroResidencia":None,
        "Email":objpaciente['DSC_EMAIL'],
        "Estrangeiro":False,
        "IdPaciente":id_paciente,
        "Telefones": [{
            "IdPacienteTelefone":"",      #TODO
            "IdPaciente":id_paciente,
            "DDD":objpaciente['NUM_TELEFONE_DDD'],
            "Telefone":objpaciente['NUM_TELEFONE_NUM']
        }]
    }

    return paciente_json

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
class Vacivida_Sys :
    def __init__(self) :
        self.auth_message = "Nao autenticado"
        self.consult_message = "Sem mensagens"
        self.vacina_message = "Tomou ZERO doses"
        self.cadastro_message = "Cadastro nao iniciado"
        self.imunizar_message = "Imunizacao nao iniciada"

    # 1. Realiza autenticacao no Vacivida
    def autenticar(self, login) :

        self.headers = {
            'Connection' : 'keep-alive',
            'access-control-allow-origin' : 'http://portalvacivida.sp.gov.br/',
            'Accept' : 'application/json, text/plain, */*',
            'sec-ch-ua-mobile' : '?0',
            'User-Agent' : 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.96 Safari/537.36',
            'sec-ch-ua' : '"Chromium";v="88", "Google Chrome";v="88", ";Not A Brand";v="99"',
            'Content-Type' : 'application/json',
            'Origin' : 'https://vacivida.sp.gov.br',
            'Sec-Fetch-Site' : 'same-site',
            'Sec-Fetch-Mode' : 'cors',
            'Sec-Fetch-Dest' : 'empty',
            'Referer' : 'https://vacivida.sp.gov.br/',
            'Accept-Language' : 'pt,en-US;q=0.9,en;q=0.8',
        }
        self.data = login
        response_login = requests.post('https://servico.vacivida.sp.gov.br/Usuario/Logar', headers=self.headers,
                                       data=self.data)

        # transforma resposta em chaves
        self.login_token = json.loads(response_login.text)

        # faz leitura do token e salva como variavel
        self.login_token = self.login_token['Data']

        if self.login_token:
            self.headers = {
                'Connection' : 'keep-alive',
                'TKP' : '0',
                'AccessToken' : self.login_token,
                'sec-ch-ua-mobile' : '?0',
                'User-Agent' : 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.96 Safari/537.36',
                'Content-Type' : 'application/json',
                'access-control-allow-origin' : 'http://portalvacivida.sp.gov.br/',
                'Accept' : 'application/json, text/plain, */*',
                'sec-ch-ua' : '"Chromium";v="88", "Google Chrome";v="88", ";Not A Brand";v="99"',
                'Origin' : 'https://vacivida.sp.gov.br',
                'Sec-Fetch-Site' : 'same-site',
                'Sec-Fetch-Mode' : 'cors',
                'Sec-Fetch-Dest' : 'empty',
                'Referer' : 'https://vacivida.sp.gov.br/',
                'Accept-Language' : 'pt,en-US;q=0.9,en;q=0.8',
            }

            self.auth_message = "Autenticado!"

        else:
            self.auth_message = "Nao autenticado"
        
        return self.get_auth_message()
        

    def get_auth_message(self) :
        return self.auth_message

    # 2. Consulta CPF
    # @retry(stop_max_attempt_number=7)
    def consultacpf(self, cpf_paciente) :
        self.CPFusuario = cpf_paciente
 
        response_cpf = requests.get('https://servico.vacivida.sp.gov.br/Paciente/cnsoucpf/'+self.CPFusuario,
                                    headers=self.headers, timeout=100)
        time.sleep(2)
        resp_text = json.loads(response_cpf.text)

        # printa json inteiro:
        # print( json.dumps(resp_text, indent=4) ) 
        
        # print(resp_text['Message'], "CPF é: ", self.CPFusuario)
        # print(dados_cadastro['Message'])
        if ("Consulta realizada com sucesso! OBS.: Nenhum dado localizado com os Parâmetros enviados." in
                resp_text['Message']) :
            # print("Usuário NÃO Cadastrado! CPF = ", self.CPFusuario)
            self.consult_message = ("Usuário NÃO Cadastrado! CPF = "+self.CPFusuario)

            return None

        elif ("Consulta realizada com sucesso!" in resp_text['Message']) :
            # print("Usuário Cadastrado! CPF = ", self.CPFusuario)
            # print('ID do Paciente = ' + resp_text['Data']['IdPaciente'])
            # print('Nome do Paciente = ' + resp_text['Data']['Nome'])
            # print('CPF = ' + resp_text['Data']['CPF'])
            self.consult_message = ("Usuário Cadastrado! CPF = "+self.CPFusuario)
            self.id_paciente = resp_text['Data']['IdPaciente']

            return resp_text["Data"]  #extrai somente  "Data" da response

        else :
            # print ("Erro na consulta do CPF!", self.CPFusuario)
            self.consult_message = "Erro na consulta do CPF!"+self.CPFusuario

            return None

    # 3. Consulta vacinacao para verificar se ja foi aplicado alguma vacina
    def consultavacinacao(self, paciente) :
        self.id_paciente = paciente['ID_PACIENTE']
        self.cpf_paciente = paciente['NUM_CPF']

        # CPFusuario=str(CPFusuario)

 
        response_vacinacao = requests.get(
            'https://servico.vacivida.sp.gov.br/Vacinacao/Historico-Vacinacao-Paciente/'+self.id_paciente,
            headers=self.headers, timeout=500)
        dados_vacinacao = json.loads(response_vacinacao.text)

        # 3.1 Notifica quantas doses foram tomadas e cria flag "vacinado"
        if len(dados_vacinacao['Data']['vacinacao']) == 0 :
            # print("CPF = ", self.cpf_paciente," Nao vacinado!")
            self.vacina_messsage = "CPF = "+self.cpf_paciente+" Tomou ZERO doses"
            vacinado = 0
        elif len(dados_vacinacao['Data']['vacinacao']) == 1 :
            # print("CPF = ", self.cpf_paciente," Tomou 1 dose")
            self.vacina_messsage = "CPF = "+self.cpf_paciente+" Tomou UMA dose"
            vacinado = 1
        elif len(dados_vacinacao['Data']['vacinacao']) == 2 :
            # print("CPF = ", self.cpf_paciente," Tomou 2 doses")
            self.vacina_messsage = "CPF = "+self.cpf_paciente+" Tomou DUAS doses"
            vacinado = 2
        # else:
        # print("CPF = ", self.cpf_paciente," Erro! Mais de 2 doses no registro")
        # print("CPF = ", self.cpf_paciente," Imunobiologico = " + dados_vacinacao['Data']['vacinacao'][0]['Imunobiologico'])

    def get_vacina_message(self) :
        return self.vacina_message

    def get_id_paciente(self) :
        return self.id_paciente

    def get_consult_message(self) :
        return self.consult_message

    # 4. Realiza cadastro do paciente
    def cadastrar_paciente(self, objpaciente) :
        self.objpaciente = objpaciente
 
        self.datacadastro = {
            "Data":parse_paciente_json(objpaciente),
            "AccessToken":self.login_token
        }

        time.sleep(5)
        #print("DEBUG - Data Cadastro = ", json.dumps(self.datacadastro, indent=4) )
        #print( json.dumps(parse_paciente_json(objpaciente),indent=4) )
        # requests.post
        response_incluir = requests.post('https://servico.vacivida.sp.gov.br/Paciente/incluir-paciente',
                                         headers=self.headers, json=self.datacadastro, timeout=500)
        time.sleep(5)
        # print("DEBUG - Response incluir= ", response_incluir)
        self.response_incluir = response_incluir
        self.dados_incluir = json.loads(response_incluir.text)
        # print("DEBUG - Dados incluir= ",self.dados_incluir)
        # print(self.dados_incluir)  #printa resposta recebida da solicitacao de cadastro

        datastring = json.dumps(self.datacadastro)

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
    def imunizar(self, objimunizacao) :
        self.objimunizacao = objimunizacao
         # print('LOG! = ', self.objimunizacao)

        imunizar_json = {
            "IdGrupoAtendimento":str(objimunizacao["DSC_PUBLICO"]),
            "IdEstrategia":objimunizacao["ESTRATEGIA"],
            "IdImunobiologico":objimunizacao["DSC_TIPO_VACINA"],
            "IdDose":objimunizacao["NUM_DOSE_VACINA"],
            "DataVacinacao":objimunizacao["DTA_COMPARECIMENTO_PESSOA"],
            "DataAprazamento":objimunizacao["DTA_APRAZAMENTO"],
            "IdLote":objimunizacao["NUM_LOTE_VACINA"],
            "IdViaAdministracao":objimunizacao["VIA_ADMINISTRACAO"],
            "IdLocalAplicacao":objimunizacao["LOCAL_APLICACAO"],
            "IdVacinador":objimunizacao["VACINADOR"],
            "IdPaciente":objimunizacao["ID_PACIENTE"],
            "IdEstabelecimento":objimunizacao["ESTABELECIMENTO"]

        }
        if (objimunizacao['DSC_COMORBIDADES'] != "null") :
            imunizar_json["IdComorbidade"]          = [ objimunizacao["COMORBSTRING"] ],
            imunizar_json["CRMComorbidade"]         = objimunizacao["NUM_CRM"],
            imunizar_json["VacinacaoComorbidade"]   = [ objimunizacao["COMORBDICT"] ],
            imunizar_json["DescricaoBPC"]           = None


        self.data_imunizar = {
            "Data":imunizar_json,
            "AccessToken":self.login_token
        }

        time.sleep(5)
        # print("LOG - DATA IMUNIZAR = "+ self.data_imunizar)
        response_imunizar = requests.post('https://servico.vacivida.sp.gov.br/Vacinacao/Inserir-Vacinacao',
                                          headers=self.headers, json=self.data_imunizar, timeout=500)
        time.sleep(5)
        # print(response_imunizar)
        self.response_imunizar = response_imunizar

        self.response_imunizar = json.loads(self.response_imunizar.text)

        if (self.response_imunizar['ValidationSummary'] != None) :
            # print(self.dados_incluir['ValidationSummary']['Erros'][0]['ErrorMessage'])
            self.imunizar_message = str(
                self.response_imunizar['ValidationSummary']['Erros'][0]['ErrorMessage'])+" CPF : "+\
                                    self.objimunizacao['NUM_CPF']
        elif ("Incluído com Sucesso" in self.response_imunizar['Message']) :
            # print("Incluido com sucesso")
            self.imunizar_message = str(self.response_imunizar['Message'])+" CPF: "+self.objimunizacao[
                'NUM_CPF']+" imunizado "

    def get_imunizar_message(self) :
        return self.imunizar_message

  # 6. Atualizar cadastro do paciente
    def atualizar_paciente(self, objpaciente, paciente_json=None, id_paciente=None) :    #obricatório paciente_json OU id_paciente
        if paciente_json == None:
            if id_paciente == None:
                return False
            else:
                paciente_json = parse_paciente_json(objpaciente)
                paciente_json["IdPaciente"] = id_paciente
        else:
            paciente_json["PesqCNS_CPF"] = objpaciente['NUM_CPF']
            paciente_json["CNS"] = None
            paciente_json["CPF"] =  objpaciente['NUM_CPF']
            paciente_json["Nome"] = objpaciente['DSC_NOME']
            paciente_json["NomeMae"] = objpaciente['DSC_NOME_MAE']
            paciente_json["NomeSocial"] = None
            paciente_json["DataNascimento"] = objpaciente['DTA_NASCIMENTO']
            paciente_json["CodigoSexo"] = objpaciente['TPO_SEXO']
            paciente_json["IdRaca"] = objpaciente['DSC_RACA_COR']
            paciente_json["IdEtnia"] = None
            paciente_json["NumeroTelefone"] = objpaciente['NUM_TELEFONE']
            paciente_json["Gestante"] = +objpaciente['GESTANTE']
            paciente_json["Puerpera"] = +objpaciente['PUERPERA']
            paciente_json["IdPaisResidencia"] = objpaciente['PAIS']
            paciente_json["IdUFResidencia"] = objpaciente['UF']
            paciente_json["IdMunicipioResidencia"] = objpaciente['MUNICIPIO']
            paciente_json["ZonaMoradia"] = objpaciente['ZONA']
            paciente_json["LogradouroResidencia"] = objpaciente['DSC_ENDERECO']
            #paciente_json["NumeroLogradouroResidencia"] = objpaciente['NUM_ENDERECO']   #TODO: implementar endereço. Por enquanto, utilizar o do pré cadastro do vacivida
            #paciente_json["Bairro"] = objpaciente['BAIRRO']
            #paciente_json["ComplementoLogradouroResidencia"] = None
            paciente_json["Email"] = objpaciente['DSC_EMAIL']
            paciente_json["Estrangeiro"] = False
            #paciente_json["IdPaciente"] = id_paciente          #utilizar o id do json passado como parâmetro
            paciente_json["NumeroTelefone"] = objpaciente['NUM_TELEFONE_DDD'] + objpaciente['NUM_TELEFONE_NUM']
            paciente_json["Telefones"][0]["DDD"] = objpaciente['NUM_TELEFONE_DDD']
            paciente_json["Telefones"][0]["Telefone"] = objpaciente['NUM_TELEFONE_NUM']

        data = {
            "Data":paciente_json,
            "AccessToken":self.login_token
        }

        time.sleep(5)
        #print("DEBUG - Data update = ", data)

        resp = requests.put('https://servico.vacivida.sp.gov.br/Paciente/atualizar-paciente',
                                         headers=self.headers, json=data, timeout=500)
        time.sleep(5)
        resp_text = json.loads(resp.text) 

        if (resp_text['ValidationSummary'] != None) :
            # print(resp_text['ValidationSummary']['Erros'][0]['ErrorMessage'])
            atualizacao_message = str(resp_text['ValidationSummary']['Erros'][0]['ErrorMessage'])+" CPF : "+\
                                    paciente_json['CPF']
        elif ("Paciente Atualizado com Sucesso!" in resp_text['Message']) :
            # print("Atualizado com sucesso")
            atualizacao_message = str(resp_text['Message']) + " CPF: "+paciente_json['CPF'] + " atualizado "
        else:
            atualizacao_message = f"Resposta da atualização: \n{json.dumps(resp_text, indent=4)}"

        return resp_text["Data"], atualizacao_message  #return paciente_json da response

    def get_lotes_vacina(self, vacina):
        vacina_id = di.vacina_id[vacina]
        #TODO     
        resp = requests.get('https://servico.vacivida.sp.gov.br/Cadastro/consulta-lote/'+vacina_id,
                                         headers=self.headers, timeout=500)
        resp_text = json.loads(resp.text) 
        # print(json.dumps(resp_text, indent=4))
        return vacina, resp_text["Data"]

@ray.remote
def worker(message_actor, j, cadastros_to_send, login) :
    a = time.time()
    d = time.time()
    time.sleep(round(random.uniform(0, 2), 2))  # jitter aumenta tempo de espera em um numero randomico entre 0 e 1
    vacivida = Vacivida_Sys.remote()
    vacivida.autenticar.remote(login)
    # print (ray.get(vacivida.get_auth_message.remote()))
    auth_message = ray.get(vacivida.get_auth_message.remote())
    # message_actor.add_message.remote("Worker {} = {}.".format(j, auth_message ))
    print("Worker ", j, auth_message)
    cadastros_worker = cadastros_to_send[j]
    time.sleep(2)
    for i in cadastros_worker :
        d = time.time()

        # habilita loop de autenticacao a cada d-a segundos
        if (d-a > 1800) :
            time.sleep(
                round(random.uniform(0, 2), 2))  # jitter aumenta tempo de espera em um numero randomico entre 0 e 1
            d = time.time()
            a = time.time()
            print("Passaram 30min, fazendo nova autenticacao para o Worker ", j)
            vacivida.autenticar.remote(login)
            auth_message = ray.get(vacivida.get_auth_message.remote())
            print("Worker ", j, auth_message)
            cadastros_worker = cadastros_to_send[j]

        # print(i['SEQ_AGENDA'])
        time.sleep(1)
        seqtoprint = i['SEQ_AGENDA']
        cpftoprint = i['NUM_CPF']
        # message_actor.add_message.remote(
        #    "SEQ_AGENDA = {} , NUM_CPF = {} reading at worker {}.".format(seqtoprint, cpftoprint, j))

        paciente_json_future = vacivida.consultacpf.remote(i['NUM_CPF'])
        cadastro_status = ray.get(vacivida.get_consult_message.remote(), timeout=60)

        print(cadastro_status)

        if ("Usuário NÃO Cadastrado" in cadastro_status) :
            con = cx_Oracle.connect(connection_params)
            cur = con.cursor()
            cur.execute(
                f"UPDATE age_agendamento_covid set ind_vacivida_cadastro ='F' where SEQ_AGENDA = {i['SEQ_AGENDA']} ")
            # print("DEBUG - Cadastro SEQ_AGENDA = ", i['SEQ_AGENDA'], " atualizado para False")

            cur.execute(
                f"UPDATE age_agendamento_covid set ind_vacivida_vacinacao ='F' where SEQ_AGENDA = {i['SEQ_AGENDA']} ")
            # print("DEBUG - Vacinacao SEQ_AGENDA = ", i['SEQ_AGENDA'], " atualizado para False")

            con.commit()
            con.close()

            # max retry default = 5
            for x in range(MAX_RETRY) :
                time.sleep(3 * x)  # backoff = tentativas * multiplicacao do tmepo de espera
                time.sleep(
                    round(random.uniform(0, 1), 2))  # jitter aumenta tempo de espera em um numero randomico entre 0 e 1

                # print("DEBUG = Cadastrar paciente = ", i)
                vacivida.cadastrar_paciente.remote(i)
                cadastrar_status = ray.get(vacivida.get_cadastro_message.remote(), timeout=50)
                print(cadastrar_status)
                if ("cadastrado" in cadastrar_status) :
                    con = cx_Oracle.connect(connection_params)
                    cur = con.cursor()
                    cur.execute(
                        f"UPDATE age_agendamento_covid set ind_vacivida_cadastro ='T' where SEQ_AGENDA = {i['SEQ_AGENDA']} ")

                    con.commit()
                    con.close()
                    break
                # adicionar vacina a partir daqui dentro de um if
                elif ("Erro" in cadastrar_status) :
                    pass

            # on.commit()
            # con.close()

            # update no bd que nao esta cadastrado
        elif ("Usuário Cadastrado") in cadastro_status :
            paciente_json = ray.get(paciente_json_future) 
            if paciente_json["CodigoSexo"] == None:     #pré-cadastro do vacivida apresenda dados inconsistentes e precisa ser atualizado
                print("Necessário atualizar o usuário")
                atualizar_future = vacivida.atualizar_paciente.remote(i, paciente_json )                
                paciente_json, atualizacao_message = ray.get(atualizar_future)
                print(atualizacao_message)

            i['ID_PACIENTE'] = paciente_json["IdPaciente"]
            # print("DEBUG - ID_PACIENTE no vacivida= ", i['ID_PACIENTE'])

            # falta inserir coluna com ID usuario para cruzar no vacivida mais rapido

            con = cx_Oracle.connect(connection_params)
            cur = con.cursor()
            cur.execute(
                f"UPDATE age_agendamento_covid set ind_vacivida_cadastro ='T' where SEQ_AGENDA = {i['SEQ_AGENDA']} ")
            print("Cadastro SEQ_AGENDA = ", i['SEQ_AGENDA'], " atualizado para True")
            con.commit()
            con.close()
            time.sleep(2)

            # for result in cur:
            # print(result)

            # Cadastrar Imunizacao
            #paciente_json = ray.get(paciente_json_future)   #recebe o json atualizado
            # comentar essa area para apenas atualizar o banco de dados
            for x in range(MAX_RETRY) :
                time.sleep(2 * x)  # backoff = tentativas * multiplicacao do tmepo de espera
                time.sleep(
                    round(random.uniform(0, 1), 2))  # jitter aumenta tempo de espera em um numero randomico entre 0 e 1
                # print("DEBUG - Object to imunizar = ", i)

                vacivida.imunizar.remote(i)
                imunizar_status = ray.get(vacivida.get_imunizar_message.remote(), timeout=60)
                print(imunizar_status)
                if ("Incluído com Sucesso" in imunizar_status) :
                    con = cx_Oracle.connect(connection_params)
                    cur = con.cursor()
                    cur.execute(
                        f"UPDATE age_agendamento_covid set ind_vacivida_vacinacao ='T' where SEQ_AGENDA = {i['SEQ_AGENDA']} ")

                    print("Vacinacao SEQ_AGENDA = ", i['SEQ_AGENDA'], " atualizado para True")
                    con.commit()
                    con.close()
                    break
                elif ("já tomou esta dose" in imunizar_status) :
                    con = cx_Oracle.connect(connection_params)
                    cur = con.cursor()
                    cur.execute(
                        f"UPDATE age_agendamento_covid set ind_vacivida_vacinacao ='T' where SEQ_AGENDA = {i['SEQ_AGENDA']} ")

                    print("Vacinacao SEQ_AGENDA = ", i['SEQ_AGENDA'], " atualizado para True")
                    con.commit()
                    con.close()
                    break

                # para o caso de ja ter sido cadastrado um tipo de vacina diferente, registra no BD como "E" (erro)
                elif ("Não é permitido que a 2ª dose da vacina seja diferente da 1ª dose CPF" in imunizar_status) :
                    con = cx_Oracle.connect(connection_params)
                    cur = con.cursor()
                    cur.execute(
                        f"UPDATE age_agendamento_covid set ind_vacivida_vacinacao ='E' where SEQ_AGENDA = {i['SEQ_AGENDA']} ")

                    print("Vacinacao SEQ_AGENDA = ", i['SEQ_AGENDA'], " atualizado para Erro")
                    con.commit()
                    con.close()
                    break

                elif ("A primeira dose do paciente não está registrado no VaciVida." in imunizar_status) :
                    con = cx_Oracle.connect(connection_params)
                    cur = con.cursor()
                    cur.execute(
                        f"UPDATE age_agendamento_covid set ind_vacivida_vacinacao ='I' where SEQ_AGENDA = {i['SEQ_AGENDA']} ")

                    print("Vacinacao SEQ_AGENDA = ", i['SEQ_AGENDA'], " atualizado para Inconsistente")
                    con.commit()
                    con.close()
                    break

                elif ("Não é permitido vacinar paciente menor de 18 anos de idade" in imunizar_status) :
                    con = cx_Oracle.connect(connection_params)
                    cur = con.cursor()
                    cur.execute(
                        f"UPDATE age_agendamento_covid set ind_vacivida_vacinacao ='X' where SEQ_AGENDA = {i['SEQ_AGENDA']} ")

                    print("Vacinacao SEQ_AGENDA = ", i['SEQ_AGENDA'], " atualizado para Data de Nascimento Incorreto")
                    con.commit()
                    con.close()
                    break

                elif ("Erro" in imunizar_status) :
                    pass

def fetch_lotes(login):
    vacivida = Vacivida_Sys.remote()
    auth_future = vacivida.autenticar.remote(login)

    lotes_local = { key:{} for key in di.vacina_id}

    auth_message = ray.get(auth_future)
    response_future = [vacivida.get_lotes_vacina.remote(vacina) for vacina in lotes_local]
    print("Scrapper de lotes ", auth_message)
    
    resp = ray.get(response_future)

    #salva lotes p/ consulta
    with open("lotes.json", "w") as fp:
        json.dump(resp, fp, indent=4)
    
    for vacina in resp:
        for lote in (vacina[1]):
            lotes_local[vacina[0]][ lote["CodigoLote"] ] = lote["IdLote"]

    # Formato do dicionario lotes_local:
    # { vacina:{cod_lote:id_lote} }
    return lotes_local

# Create a message actor.
message_actor = MessageActor.remote()


# Loop para reiniciar os registros a cada time.sleep(x) tempo
# Dica: em momentos de instabilidade, pode ser interessante reduzir o tempo para apenas alguns minutos
while True :
    while True:
        print("Atualizando listas de lotes")
        try:
            lotes = fetch_lotes( list(login_vacivida.values())[0] )     #utiliza o login da primeira unidade
            print("Lista de lotes atualizada")
            break
        except:
            print("Não foi possível atualizar lista de lotes.")
        time.sleep(60)

    for area in di.area_alias:        
        print(f"[{area}] Inicializando processos...")
        list_to_send, list_seq_agenda = GetDB(area)
        alias = di.area_alias[area]     #diferentes areas (DSC_AREA) representam o mesmo local (alias)

        if len(list_seq_agenda) != 0 and di.vacinador[alias] != "" and di.estabelecimento[alias] != "":
            registers_to_send = CreateRegistersToSend(list_to_send)

            [worker.remote(message_actor, j, registers_to_send, login_vacivida[alias]) for j in range(n_workers)]

            new_messages = ray.get(message_actor.get_and_clear_messages.remote())
            print("New messages:", new_messages)

            time.sleep(60)  # aguarda X segundos para despachar workers de nova area

    time.sleep(3600)  # reinicia processos a cada X segundos