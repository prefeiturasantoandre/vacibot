import json, requests, time
from dicts import vacina_id

def parse_paciente_json(objpaciente, id_paciente=""):
    paciente_json = {
        "PesqCNS_CPF":objpaciente['NUM_CPF'],
        "CNS":objpaciente['NUM_CNS'],
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

# Funcoes referentes ao Vacivida
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
            'Content-Type' : 'application/json;charset=utf-8',
            'Origin' : 'https://vacivida.sp.gov.br',
            'Sec-Fetch-Site' : 'same-site',
            'Sec-Fetch-Mode' : 'cors',
            'Sec-Fetch-Dest' : 'empty',
            'Referer' : 'https://vacivida.sp.gov.br/',
            'Accept-Language' : 'pt,en-US;q=0.9,en;q=0.8',
        }

        data = {"Data":{
            "Login":login[0],
            "Senha":login[1]
        }}
        response_login = requests.post('https://servico.vacivida.sp.gov.br/Usuario/Logar', headers=self.headers,
                                       json=data)

        # transforma resposta em chaves
        resp_text = json.loads(response_login.text)

        # faz leitura do token e salva como variavel

        if resp_text['Data']:
            self.login_token = resp_text['Data']
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
            self.auth_message = "Nao autenticado: " + str(resp_text['ValidationSummary']['Erros'][0]['ErrorMessage'])
        
        return self.get_auth_message()
        

    def get_auth_message(self) :
        return self.auth_message

    # 2. Consulta CPF
    # @retry(stop_max_attempt_number=7)
    def consultacpf(self, cpf_paciente) :
        self.CPFusuario = str(cpf_paciente)
 
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
        paciente_json = parse_paciente_json(objpaciente)
 
        self.datacadastro = {
            "Data":paciente_json,
            "AccessToken":self.login_token
        }

        time.sleep(5)
        #print("DEBUG - Data Cadastro = ", json.dumps(self.datacadastro, indent=4) )
        #print( json.dumps(parse_paciente_json(objpaciente),indent=4) )
        # requests.post
        response_incluir = requests.post('https://servico.vacivida.sp.gov.br/Paciente/incluir-paciente',
                                         headers=self.headers, json=self.datacadastro, timeout=500)
        
        resp_text = json.loads(response_incluir.text) 

        if (resp_text['ValidationSummary'] != None) :
            cadastro_message = str(resp_text['ValidationSummary']['Erros'][0]['ErrorMessage']) + " CPF : "+ paciente_json['CPF']
        elif ("Incluído com Sucesso" in resp_text['Message']) :
            cadastro_message = str(resp_text['Message']) + " CPF: "+paciente_json['CPF'] + " cadastrado"
        else:
            cadastro_message = f"Resposta do cadastro: \n{json.dumps(resp_text, indent=4)}"

        return resp_text["Data"], cadastro_message  #return paciente_json da response

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
            "IdEstabelecimento":objimunizacao["ESTABELECIMENTO"],
            "IdMotivoDoseAdicional":None,
            "IdPaisPrimeiraDose":None,
            "IdUFPrimeiraDose":None,
            "PrimeiraDoseOutroEstado":None,
            "PrimeiraDoseOutroPais":None,
        }
        from dicts import grupo_id
        if (objimunizacao['DSC_PUBLICO'] == grupo_id["COMORBIDADE"]) :
            imunizar_json["IdComorbidade"]          = objimunizacao["COMORBLIST"]
            imunizar_json["CRMComorbidade"]         = objimunizacao["NUM_CRM"]
            imunizar_json["VacinacaoComorbidade"]   = [ {"IdComorbidade":comorb} for comorb in objimunizacao["COMORBLIST"] ]
            imunizar_json["DescricaoBPC"]           = None


        self.data_imunizar = {
            "Data":imunizar_json,
            "AccessToken":self.login_token
        }

        #print( json.dumps(self.data_imunizar, indent=4))
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
    def atualizar_paciente(self, objpaciente, paciente_json=None, id_paciente=None) :    #obrigatório paciente_json OU id_paciente
        if paciente_json == None:
            if id_paciente == None:
                return None, f"Erro ao atualizar paciente. CPF: {objpaciente['NUM_CPF']}"
            else:
                paciente_json = parse_paciente_json(objpaciente)
                paciente_json["IdPaciente"] = id_paciente
        else:
            paciente_json["PesqCNS_CPF"] = objpaciente['NUM_CPF']
            paciente_json["CNS"] = objpaciente['NUM_CNS']
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
            if paciente_json["Telefones"]:
                paciente_json["Telefones"][0], msg_telefone = self.atualizar_telefone_paciente( paciente_json["IdPaciente"], objpaciente['NUM_TELEFONE_DDD'], objpaciente['NUM_TELEFONE_NUM'], paciente_json["Telefones"][0]["IdPacienteTelefone"] )
            else:
                paciente_json["Telefones"] = msg_telefone = [ self.atualizar_telefone_paciente(paciente_json["IdPaciente"], objpaciente['NUM_TELEFONE_DDD'], objpaciente['NUM_TELEFONE_NUM']) ]
            
            # verifica se houv erro ao atualizar telefone
            if "Telefone Atualizado com Sucesso" not in msg_telefone:
                return None, msg_telefone
        data = {
            "Data":paciente_json,
            "AccessToken":self.login_token
        }

        time.sleep(5)
        #print("DEBUG - Data update = ", data)

        resp = requests.put('https://servico.vacivida.sp.gov.br/Paciente/atualizar-paciente',
                                         headers=self.headers, json=data, timeout=500)
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

        #retorna paciente_json da response // paciente_json será null se ocorrer erro na atualização
        return resp_text["Data"], atualizacao_message

    def get_lotes_vacina(self, vacina):
        vac_id = vacina_id[vacina]     
        resp = requests.get('https://servico.vacivida.sp.gov.br/Cadastro/consulta-lote/'+vac_id,
                                         headers=self.headers, timeout=500)
        resp_text = json.loads(resp.text) 
        # print(json.dumps(resp_text, indent=4))
        return vacina, resp_text["Data"]

    def atualizar_telefone_paciente(self, id_paciente, ddd, phone, id_paciente_telefone=None):
        telefone_json = {
            "IdPacienteTelefone":id_paciente_telefone,
            "IdPaciente":id_paciente,
            "DDD":str(ddd),
            "Telefone":str(phone)
        }

        data = {
            "Data":telefone_json,
            "AccessToken":self.login_token
        }

        resp = requests.put('https://servico.vacivida.sp.gov.br/Paciente/atualizar-telefone-paciente',
                                         headers=self.headers, json=data, timeout=500)
        resp_text = json.loads(resp.text) 

        if (resp_text['ValidationSummary'] != None) :
            atualizacao_message = str(resp_text['ValidationSummary']['Erros'][0]['ErrorMessage'])
        elif ("Telefone Atualizado com Sucesso" in resp_text['Message']) :
            atualizacao_message = str(resp_text['Message'])
        else:
            atualizacao_message = f"Resposta da atualização: \n{json.dumps(resp_text, indent=4)}"

        return resp_text["Data"], atualizacao_message  #return telefone_json da response

    def get_historico_vacinacao(self, paciente_id):
        resp = requests.get('https://servico.vacivida.sp.gov.br/Vacinacao/Historico-Vacinacao-Paciente-Tela/' + paciente_id, 
                            headers=self.headers, timeout=500)
        resp_text = json.loads(resp.text) 
        return resp_text["Data"]["vacinacao"]
    
    def delete_vacinacao(self, vacinacao_id):
        resp = requests.put('https://servico.vacivida.sp.gov.br/Vacinacao/deletar-vacinacao/' + vacinacao_id, 
                            headers=self.headers, timeout=500)

        if resp.status_code != requests.codes.ok:
            return False, f"{resp.status_code} - Erro durante o Request de exclusão"
        resp_text = json.loads(resp.text) 

        if (resp_text['ValidationSummary'] != None) :
            msg = str(resp_text['ValidationSummary']['Erros'][0]['ErrorMessage'])
            success = False
        elif ("Registro excluído com Sucesso!" in resp_text['Message']) :
            msg = str(resp_text['Message'])
            success = True
        else:
            msg = f"Resposta da exclusão: \n{json.dumps(resp_text, indent=4)}"
            success = False
        return success, msg
