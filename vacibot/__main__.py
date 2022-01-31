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
from settings import db, MAX_RETRY, MAX_WORKERS
from vacivida import Vacivida_Sys

from worker import Manager
Manager_remote = ray.remote(Manager)

# DEFINES
max_workers = MAX_WORKERS  # recomendado nao passar de 40 workers por credentials.py
standalonemode = True
headnodeip = '127.0.0.1:6379'
headnodepassword = '5241590000000000'

#argument for n_workers
try:
    max_workers = int(sys.argv[1])
except:
    pass

# Global variables
table_index = []            #Nomes das colunas da select_query
global lotes
lotes = {}
global dados_base
dados_base = {}


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

    lista_dividida = np.array_split(list_seq_agenda, 1)  # divide a lista de seqs pela quantidade de workers

    # Coloca o resultado do split em list_to_send de acordo com a qntde de workers
    for array in lista_dividida :
        # print (array)
        list_to_send.append(array.tolist())

    # tamanho da lista sera utilizado para definir o tamanho do loop dos workers
    size_of_list = len(list_seq_agenda) / 1

    print(f"########## [{filter_area}] Tamanho da lista: ", len(list_seq_agenda))

    return list_to_send, list_seq_agenda


# Cria classe RegisterBatch para poder realizar operacoes em cima de forma mais facil
class RegisterBatch() :
    def __init__(self, list_agenda, list_index) :
        self.list_agenda = list_agenda      #sublista de list_to_send (ie, lista de registros para um worker)
        self.list_index = list_index        #table_index (cabecalho das colunas)

    def parse_to_dict(self, ) :
        self.list_agenda_parsed = []
        for list_agenda_line in self.list_agenda :
            parser_error = None
            try:
                self.dict = {self.list_index[0] : str(list_agenda_line[0]),  # SEQ_AGENDA
                            self.list_index[1] : str(list_agenda_line[1]),  # new: DSC_PUBLICO // old: 'DSC_AREA'
                            self.list_index[2] : str(list_agenda_line[2]),  # 'DSC_NOME'
                            self.list_index[3] : str(list_agenda_line[3]),  # 'NUM_CPF'
                            self.list_index[4] : str(list_agenda_line[4]),  # 'DSC_NOME_MAE'
                            self.list_index[5] : list_agenda_line[5],  # 'NUM_CNS'
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
                            self.list_index[21] : list_agenda_line[21],  # 'NUM_CRM'
                            self.list_index[22] : str(list_agenda_line[22]),  # 'DSC_COMORBIDADES'
                            self.list_index[23] : str(list_agenda_line[23]),  # 'IND_VACIVIDA_CADASTRO'
                            self.list_index[24] : str(list_agenda_line[24]),  # 'IND_VACIVIDA_VACINACAO'
                            self.list_index[25] : str(list_agenda_line[25]),  # 'DSC_AREA'
                            self.list_index[26] : str(list_agenda_line[26]),  # 'DS_GRUPO_ATENDIMENTO'
                            self.list_index[27] : (list_agenda_line[27]),  # 'DSC_OUTRA_CIDADE'
                            self.list_index[28] : (list_agenda_line[28]),  # 'DSC_OUTRO_ESTADO'
                            self.list_index[29] : (list_agenda_line[29]),  # 'DSC_OUTRO_PAIS'
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
                dta_nascimento = self.dict['DTA_NASCIMENTO']
                self.dict['DTA_NASCIMENTO'] = dta_nascimento.strftime("%Y-%m-%dT%H:%M:%S.000Z")

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

                #### INFOS PARA VACINACAO
                # fixos:
                self.dict['ESTRATEGIA'] = "B66C4B622F6E840AE053D065C70A17A1"
                self.dict['VIA_ADMINISTRACAO'] = "B66C4B622F6B840AE053D065C70A17A1"
                self.dict['LOCAL_APLICACAO'] = "B66C4B622F25840AE053D065C70A17A1"

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


                self.comorbset = set()
                # parse grupo
                # regras especiais
                if ("SÍNDROME DE DOWN" in self.dict['DSC_PUBLICO']) :
                    self.comorbset.add(di.comorbidade_id["SINDROME DE DOWN"])
                elif ("HEMODIÁLISE" in self.dict['DSC_PUBLICO']) :
                    # hemodialise -> doenca renal cronica
                    self.comorbset.add(di.comorbidade_id["DOENCA RENAL CRONICA"])
                elif ("IMUNOSSUPRESSOR" in self.dict['DSC_PUBLICO']) :
                    self.comorbset.add(di.comorbidade_id["IMUNOSSUPRIMIDO"])
                elif ("GESTANTES, PUÉRPERAS E LACTANTES" in self.dict['DSC_PUBLICO']) :    
                    self.dict['TPO_SEXO'] = "F"
                    if( not ( ("GESTANTE" in self.dict['DSC_COMORBIDADES']) or ("PUÉRPERA" in self.dict['DSC_COMORBIDADES']) or ("LACTANTE" in self.dict['DSC_COMORBIDADES'])) ): #se não foi coletado a distinção, atribuir 'gestante'
                        self.dict['GESTANTE'] = True

                # grupo
                if self.dict['DS_GRUPO_ATENDIMENTO'] in di.grupo_id:
                    self.dict['DSC_PUBLICO'] = di.grupo_id[self.dict['DS_GRUPO_ATENDIMENTO']]

                    if self.dict['DS_GRUPO_ATENDIMENTO'] == 'COMORBIDADE':
                        for comorb in di.comorbidade_db:
                            #para cada comorbidade no banco de dados, adiciona id do vacivida ao self.comorbset
                            if comorb in self.dict['DSC_COMORBIDADES'] :
                                self.comorbset.add(di.comorbidade_id[ di.comorbidade_db[comorb] ])
                        # verifica o CRM p/ um válido
                        if ( (not self.dict['NUM_CRM']) or self.dict['NUM_CRM'] == '0' ):
                            self.dict['NUM_CRM'] = "40787"
                        else:
                            self.dict['NUM_CRM'] = re.sub(r'\D','', self.dict['NUM_CRM'] )   #Formata p/ somente numeros
                    
                    # QUANDO EXISTIR A INFORMAÇÃO NA LISTA DE "COMORBIDADES" OS VALORES INDIGENA e QUILOMBOLA
                    # SETA O ID GRUPO ACORDO COM O VALOR ENCONTRADO
                    elif "Indígena" in self.dict['DSC_COMORBIDADES'] :
                        self.dict['DSC_PUBLICO'] = di.grupo_id['INDIGENAS']
                    elif "Quilombola" in self.dict['DSC_COMORBIDADES'] :
                        self.dict['DSC_PUBLICO'] = di.grupo_id['QUILOMBOLA']   

                elif self.dict['NUM_DOSE_VACINA'] == '3':
                    pass    #grupo será replicado pelo estado 8 do Filler
                else :
                    parser_error = f"Grupo de vacinacao nao identificado! {self.dict['DS_GRUPO_ATENDIMENTO']}"      


                self.dict['COMORBLIST'] = list(self.comorbset)

                # parse dose
                #verifica se é dose adicional
                if self.dict['NUM_DOSE_VACINA'] == '3':
                    self.dict['NUM_DOSE_VACINA'] = 'Adicional'
                    self.dict["FlagDoseAdicional"] = 1
                    if self.dict['DSC_PUBLICO'] in ( di.grupo_id['IDOSO'], di.grupo_id['IDOSO EM ILPI'] ):
                        self.dict["IdMotivoDoseAdicional"] = di.dose_adicional['PESSOA >= 60 ANOS']
                    elif di.comorbidade_id['IMUNOSSUPRIMIDO'] in self.dict['COMORBLIST']:
                        self.dict["IdMotivoDoseAdicional"] = di.dose_adicional['IMUNOSSUPRIMIDO']
                    elif self.dict['DSC_PUBLICO'] == di.grupo_id['TRABALHADOR DE SAUDE']:
                        self.dict["IdMotivoDoseAdicional"] = di.dose_adicional['TRABALHADOR DA SAÚDE']
                    elif self.dict["DSC_PUBLICO"] == "PESSOAS COM VIAGEM MARCADA PARA O EXTERIOR":
                        self.dict["IdMotivoDoseAdicional"] = di.dose_adicional['VIAGEM AO EXTERIOR']
                    elif self.dict["DSC_PUBLICO"] in (di.grupo_id["POPULACAO EM GERAL"], di.grupo_id["TRABALHADOR DA EDUCACAO"]):
                        self.dict["IdMotivoDoseAdicional"] = di.dose_adicional['POPULACAO GERAL']
                    else:
                        parser_error = f"Motivo de dose adicional não identificado | DSC_PUBLICO={self.dict['DSC_PUBLICO'] }"          

                self.dict['NUM_DOSE_VACINA'] = di.dose_id[self.dict['NUM_DOSE_VACINA']]
                if ("JANSSEN" in self.dict['DSC_TIPO_VACINA'] or "Janssen" in self.dict['DSC_TIPO_VACINA']) and self.dict['NUM_DOSE_VACINA'] != di.dose_id['Adicional']:
                    self.dict['NUM_DOSE_VACINA'] = di.dose_id["Unica"]       #dose única


                # parse tipo de vacina e lote
                found = False
                if self.dict['DSC_TIPO_VACINA'] == 'AstraZeneca/Fiocruz':   #trata vacina com nome diferente no db
                    self.dict['DSC_TIPO_VACINA'] = 'AstraZeneca'
                if self.dict['DSC_TIPO_VACINA'] in di.vacina_id :  #key = fabricante da vacina
                    key = self.dict['DSC_TIPO_VACINA']
                    lote = self.dict['NUM_LOTE_VACINA']
                    self.dict['DSC_TIPO_VACINA'] = di.vacina_id[key]
                    self.dict['NUM_LOTE_VACINA'] = lotes[key].get(lote)
                    found = True

                    if self.dict['NUM_LOTE_VACINA'] == None:
                        parser_error = f"Lote não identificado para {key}: {lote}"
                        
                        #salva lotes c/ erro p/ consulta
                        with open("logs/lotes_erro.csv", "a") as fp:
                            fp.write(f"{key},{lote},{datetime.now()}\n")                        
                
                if not found:    
                    parser_error = "Vacina não identificada: ", self.dict['DSC_TIPO_VACINA']
                del found


                # calcula aprazamento e parser datas
                aprazamento = None
                if self.dict["DSC_TIPO_VACINA"] == di.vacina_id["Coronavac"]:
                    aprazamento = 28
                elif self.dict["DSC_TIPO_VACINA"] == di.vacina_id["AstraZeneca"]:
                    aprazamento = 56
                elif self.dict["DSC_TIPO_VACINA"] == di.vacina_id["Pfizer"]:
                    if (datetime.today().year - dta_nascimento.year) < 18:
                        aprazamento = 56
                    else:
                        aprazamento = 21
                elif self.dict["DSC_TIPO_VACINA"] == di.vacina_id["Pfizer Pediatrico"]:
                    aprazamento = 56
                
                if aprazamento:
                    self.dict['DTA_APRAZAMENTO'] = (self.dict['DTA_COMPARECIMENTO_PESSOA']+timedelta(days=aprazamento)).strftime("%Y-%m-%dT%H:%M:%S")+".000Z"
                else:
                    # janssen
                    self.dict['DTA_APRAZAMENTO'] = self.dict['DTA_COMPARECIMENTO_PESSOA'].strftime("%Y-%m-%dT%H:%M:%S.000Z")

                # print (self.dict['DTA_COMPARECIMENTO_PESSOA'])
                self.dict['DTA_COMPARECIMENTO_PESSOA'] = self.dict['DTA_COMPARECIMENTO_PESSOA'].strftime(
                    "%Y-%m-%dT%H:%M:%S.000Z")
                self.dict['DTA_COMPARECIMENTO_PESSOA'] = str(self.dict['DTA_COMPARECIMENTO_PESSOA'])

                #parse dose anterior em outro estado/país
                if self.dict['DSC_OUTRO_ESTADO']:
                    for uf in dados_base["UFs"]:
                        if uf['SiglaUF'] == self.dict['DSC_OUTRO_ESTADO']:
                            estado = uf['IdUF']
                            break
                    if self.dict['NUM_DOSE_VACINA'] == di.dose_id['2']:
                        self.dict['PrimeiraDoseOutroEstado'] = True
                        self.dict['IdUFPrimeiraDose'] = estado
                    elif self.dict['NUM_DOSE_VACINA'] == di.dose_id['Adicional']:
                        self.dict['SegundaDoseOutroEstado'] = True
                        self.dict['IdUFSegundaDose'] = estado
                elif self.dict['DSC_OUTRO_PAIS']:
                    for p in dados_base["Paises"]:
                        if p['Pais'] == self.dict['DSC_OUTRO_PAIS']:
                            pais = p['IdPais']
                            break
                    if self.dict['NUM_DOSE_VACINA'] == di.dose_id['2']:
                        self.dict['PrimeiraDoseOutroPais'] = True
                        self.dict['IdPaisPrimeiraDose'] = pais
                    elif self.dict['NUM_DOSE_VACINA'] == di.dose_id['Adicional']:
                        self.dict['SegundaDoseOutroPais'] = True
                        self.dict['IdPaisSegundaDose'] = pais

            except Exception as e:
                parser_error = e

            # salva agenda parseada se o parser não apresentou erro
            if parser_error:
                print(f"Exception: {parser_error} | Cadastro SEQ_AGENDA={self.dict['SEQ_AGENDA']} desconsiderado")
            else:
                self.list_agenda_parsed.append(self.dict)

    def get_list_agenda_parsed_full(self) :
        return self.list_agenda_parsed

    def get_list_agenda_parsed_line(self, line) :
        return self.list_agenda_parsed[line]

    def size_list_agenda_parsed(self) :
        return len(self.list_agenda_parsed)


# Cria e transforma objetos Cadastros em listas para poder enviar para o worker depois:
def CreateRegistersToSend(list_to_send) :   #legado
    registers_to_send = []
    cadastros = []

    for i in range(len(list_to_send)) :
        cadastros.append(i)
        cadastros[i] = RegisterBatch(list_to_send[i], table_index)
        cadastros[i].parse_to_dict()
        registers_to_send.append(cadastros[i].get_list_agenda_parsed_full())

    return registers_to_send

def CreateRegistersToSend_unsplitted(list_seq_agenda) :
    registers_to_send = []
    cadastros = RegisterBatch(list_seq_agenda, table_index)
    cadastros.parse_to_dict()
    return cadastros.get_list_agenda_parsed_full()

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
    while auth_message != "Autenticado!":
        auth_message = ray.get(vacivida.autenticar.remote(login))
    
    response_future = [vacivida.get_lotes_vacina.remote(vacina) for vacina in lotes_local]
    print("Scrapper de lotes ", auth_message)
    
    resp = ray.get(response_future)
    dados_paciente_future = vacivida.get_dados_paciente.remote() # lista de possíveis valores para o cadastro de paciente

    #salva lotes p/ consulta
    with open("logs/lotes.json", "w") as fp:
        json.dump(resp, fp, indent=4)
    
    # Preenche o dicionario lotes_local com o formato:
    # { vacina:{cod_lote:id_lote} }
    for vacina in resp:
        for lote in (vacina[1]):
            lotes_local[vacina[0]][ lote["CodigoLote"] ] = lote["IdLote"]

    #atualiza os lotes no db
    ray.remote(update_lotes_db).remote(lotes_local)

    dados_paciente = ray.get( dados_paciente_future )
    return lotes_local, dados_paciente

def update_lotes_db(local_lotes):
    print("Atualizando lista de lotes no Banco de Dados")

    #cria uma cópia do dict de lotes do vacivida ao invés de usá-lo como referência
    local_lotes = dict(local_lotes)  
    for lote in local_lotes:
        local_lotes[lote] = dict(local_lotes[lote])

    #substitui a(s) chave(s) do dict pela equivalente no DB
    local_lotes['AstraZeneca/Fiocruz'] = local_lotes.pop('AstraZeneca') 

    #define tabela de lotes
    lotes_table = "AGE_LOTE_VACINA_COVID"

    #busca os lotes no db
    headers, rows = db.fetch(lotes_table)

    #busca os indices dos cabecalhos
    vacina_i = headers.index("DSC_TIPO_VACINA")
    lote_i   = headers.index("NUM_LOTE_VACINA")
    ativo_i  = headers.index("IND_ATIVO")

    for row in rows:    #para cada linha do banco de dados
        if row[ativo_i] == "S":
            # se a linha estiver ativa
            # verifica se existe equivalente no vacivida
            if row[lote_i] in local_lotes[row[vacina_i]]:
                # remove o lote do dicionário do vacivida
                local_lotes[row[vacina_i]].pop( row[lote_i] )
            else:
                # desativa o lote no banco de dados
                db.update(lotes_table, "IND_ATIVO", "N", "DSC_TIPO_VACINA", row[vacina_i], "=", "NUM_LOTE_VACINA", row[lote_i], "=")
                print("Lote desabilitado no Banco de Dados: ", row[vacina_i], row[lote_i])

                #salva lotes c/ erro p/ consulta
                with open("logs/lotes_desativados.csv", "a") as fp:
                    fp.write(f"{row[vacina_i]},{row[lote_i]},{datetime.now()}\n")

            
    # insere lotes remanescentes no banco de dados        
    for vacina in local_lotes:
        for lote in local_lotes[vacina]:
            if list(  filter(lambda row: row[ativo_i] == "N" and row[vacina_i] == vacina and row[lote_i] == lote, rows)  ):
                # se o lote estiver desativado no banco de dados, reativá-lo
                db.update(lotes_table, "IND_ATIVO", "S", "DSC_TIPO_VACINA", vacina, "=", "NUM_LOTE_VACINA", lote, "=")
                print("Lote reativado no Banco de Dados: ", vacina,lote)

                #salva lotes reativados p/ consulta
                with open("logs/lotes_reativados.csv", "a") as fp:
                    fp.write(f"{vacina},{lote},{datetime.now()}\n")
            else:
                # se o lote não existir no banco de dados, inserí-lo
                db.insert(lotes_table, ["DSC_TIPO_VACINA","NUM_LOTE_VACINA","IND_ATIVO","DTA_CRIACAO"], [vacina,lote,"S","SYSTIMESTAMP"])
                print("Lote inserido no Banco de Dados: ", vacina,lote)

                #salva lotes inseridos p/ consulta
                with open("logs/lotes_inseridos.csv", "a") as fp:
                    fp.write(f"{vacina},{lote},{datetime.now()}\n")

    print("Lista de lotes no Banco de Dados atualizada.")


# Create a message actor.
#message_actor = MessageActor.remote()

# Loop para reiniciar os registros a cada time.sleep(x) tempo
# Dica: em momentos de instabilidade, pode ser interessante reduzir o tempo para apenas alguns minutos
while __name__ == "__main__":
    try:
        while True:
            print("Atualizando listas de lotes")
            try:
                lotes, dados_base = fetch_lotes( list(login_vacivida.values())[0] )     #utiliza o login da primeira unidade
                print("Lista de lotes atualizada")
                break
            except Exception as e:
                print("Não foi possível atualizar lista de lotes.")
            time.sleep(30)

        print ("Inicializando processos...")
        try:
            list_to_send, list_seq_agenda = GetDB()
        except Exception as e:
            print("Não foi possível obter registros do banco de dados: ", e)
            time.sleep(300)
            continue
            
        records_parsed = CreateRegistersToSend_unsplitted(list_seq_agenda)
        manager = Manager_remote.options(name='manager').remote(max_workers)

        # separa os registros por area
        area_records = {}
        for record in records_parsed:
            try:
                area_records[ di.area_alias[record["DSC_AREA"]] ].append(record)
            except:
                area_records[ di.area_alias[record["DSC_AREA"]] ] = [ record ]    

        # inicializa os supervisores
        created = []
        print("Quantidade por Local:")
        for area in area_records:
            if di.vacinador[area] != "" and di.estabelecimento[area] != "":
                created.append( manager.create_supervisor.remote(area, area_records[area], login_vacivida[area]) )
                print (f"{len(area_records[area]):^5} - {area}")
        
        # aguarda a confirmação de criação dos supervisores e executa
        ray.get(created)
        manager.run.remote()
        #manager.run()

        #termina os actors e reinicia o processamento
        time.sleep(3600)  # reinicia processos a cada X segundos
        ray.kill(manager)
        
    except Exception as e:
        print("Erro durante a inicialização: ", e)
        time.sleep(3600)