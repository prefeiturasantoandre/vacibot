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
from settings import db, MAX_RETRY
from vacivida import Vacivida_Sys

from worker import Filler
Filler_remote = ray.remote(Filler)

# DEFINES
n_workers = 2  # recomendado nao passar de 40 workers por credentials.py
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

def fetch_lotes(login):
    vacivida = ray.remote(Vacivida_Sys).remote()
    auth_future = vacivida.autenticar.remote(login)

    lotes_local = { key:{} for key in di.vacina_id}

    auth_message = ray.get(auth_future)
    response_future = [vacivida.get_lotes_vacina.remote(vacina) for vacina in lotes_local]
    print("Scrapper de lotes ", auth_message)
    
    resp = ray.get(response_future)

    #salva lotes p/ consulta
    with open("lotes.json", "w") as fp:
        json.dump(resp, fp, indent=4)
    
    # Preenche o dicionario lotes_local com o formato:
    # { vacina:{cod_lote:id_lote} }
    for vacina in resp:
        for lote in (vacina[1]):
            lotes_local[vacina[0]][ lote["CodigoLote"] ] = lote["IdLote"]

    #atualiza os lotes no db
    ray.remote(update_lotes_db).remote(lotes_local)

    return lotes_local

def update_lotes_db(local_lotes):
    #cria uma cópia do dict de lotes do vacivida ao invés de usá-lo como referência
    print("Atualizando lista de lotes no Banco de Dados")
    local_lotes = dict(local_lotes)  
    for lote in local_lotes:
        local_lotes[lote] = dict(local_lotes[lote])

    #substitui a(s) chave(s) do dict pela equivalente no DB
    local_lotes['AstraZeneca/Fiocruz'] = local_lotes.pop('AstraZeneca') 

    #busca os lotes ativos no db
    headers, rows = db.fetch("AGE_LOTE_VACINA_COVID", "IND_ATIVO", "S")

    #busca os indices dos cabecalhos
    vacina_i = headers.index("DSC_TIPO_VACINA")
    lote_i   = headers.index("NUM_LOTE_VACINA")

    for row in rows:    #para cada linha do banco de dados
        # verifica se existe equivalente no vacivida
        if row[lote_i] in local_lotes[row[vacina_i]]:
            # remove o lote do dicionário do vacivida
            local_lotes[row[vacina_i]].pop( row[lote_i] )
        else:
            # desativa o lote no banco de dados
            db.update("AGE_LOTE_VACINA_COVID", "IND_ATIVO", "N", "DSC_TIPO_VACINA", row[vacina_i], "=", "NUM_LOTE_VACINA", row[lote_i], "=")
            print("Lote desabilitado no Banco de Dados: ", row[vacina_i], row[lote_i])

            #salva lotes c/ erro p/ consulta
            with open("lotes_desativados.csv", "a") as fp:
                fp.write(f"{row[vacina_i]},{row[lote_i]},{datetime.now()}\n")

            
    # insere lotes remanescentes no banco de dados        
    for vacina in local_lotes:
        for lote in local_lotes[vacina]:
            db.insert("AGE_LOTE_VACINA_COVID", ["DSC_TIPO_VACINA","NUM_LOTE_VACINA","IND_ATIVO","DTA_CRIACAO"], [vacina,lote,"S","SYSTIMESTAMP"])
            print("Lote inserido no Banco de Dados: ", vacina,lote)

            #salva lotes inseridos p/ consulta
            with open("lotes_inseridos.csv", "a") as fp:
                fp.write(f"{vacina},{lote},{datetime.now()}\n")

    print("Lista de lotes no Banco de Dados atualizada.")


# Create a message actor.
message_actor = MessageActor.remote()

# Loop para reiniciar os registros a cada time.sleep(x) tempo
# Dica: em momentos de instabilidade, pode ser interessante reduzir o tempo para apenas alguns minutos
while True :
    filler_handles = []
    while True:
        print("Atualizando listas de lotes")
        try:
            lotes = fetch_lotes( list(login_vacivida.values())[0] )     #utiliza o login da primeira unidade
            print("Lista de lotes atualizada")
            break
        except Exception as e:
            print("Não foi possível atualizar lista de lotes.")
        time.sleep(60)

    for area in di.area_alias:        
        print(f"[{area}] Inicializando processos...")
        list_to_send, list_seq_agenda = GetDB(area)
        alias = di.area_alias[area]     #diferentes areas (DSC_AREA) representam o mesmo local (alias)

        if len(list_seq_agenda) != 0 and di.vacinador[alias] != "" and di.estabelecimento[alias] != "":
            registers_to_send = CreateRegistersToSend(list_to_send)            
            
            # inicializa os actors Filler
            #Filler(0,registers_to_send, login_vacivida[alias], run=True)        #debug
            handles = [Filler_remote.remote(j, registers_to_send, login_vacivida[alias], run=False) for j in range(n_workers)]
            [h.run.remote() for h in handles]

            # agrega os novos Actor_handles de Filler_remote à lista para manter a referência e manter os Actors vivos
            filler_handles += handles
            
            new_messages = ray.get(message_actor.get_and_clear_messages.remote())
            print("New messages:", new_messages)

            time.sleep(60)  # aguarda X segundos para despachar workers de nova area

    #termina os actors e reinicia o processamento
    time.sleep(3600)  # reinicia processos a cada X segundos
    [ray.kill(h) for h in filler_handles]