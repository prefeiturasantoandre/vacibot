import time, logging, ray, asyncio
from vacivida import Vacivida_Sys
from settings import db, MAX_RETRY, WORKING_QUEUE, DISPATCHER_WAIT

logging.basicConfig(filename="logs/worker.log", level=logging.ERROR)

class Filler():
    def __init__(self, id, area, cadastros, login, run=False):
        self.id = id
        self.area = area
        self.cadastros_worker = cadastros
        self.login = login
        self.authenticated = False
        self.vacivida = Vacivida_Sys()
        self.state = 0      #0 = não executando
        self.auth_time = 0

        print("Inicializando worker ", id)

        if run:
            self.run()


    def run(self):
        while True:        # roda enquanto houver cadastros não inseridos no vacivida
            if not self.cadastros_worker:
                # se a lista de cadastros está vazia, solicita mais cadastros ao Supervisor
                print("Requisitando novos cadastros ao supervisor")
                supervisor = ray.get_actor(f"{self.area}.supervisor")
                new_entries = ray.get( supervisor.pop_entries.remote() )
                self.cadastros_worker.extend(new_entries)

                # se o supervisor não devolver novos cadastros, sai do loop
                if not self.cadastros_worker:
                    print("Lista de cadastros finalizada")
                    break
            self.working_entry = self.cadastros_worker.pop(0)
            self.state = 1
            self.working_paciente_json = None
            self.remaining_retry = 0
            while self.state not in (99,-1):         #99 = finalizado | -1 = erro
                try:
                    self.step()
                    #print(self.id,self.vacivida.login_token[-44:])     #debug
                except Exception as e:
                    self.state = -1
                    print ("CRITICAL - Um Exception lançado no processamento do cadastro.")
                    logging.exception(f"CRITICAL - Um Exception lançado no processamento do cadastro: {self.working_entry}")
        return True
            
    def step(self):
        if not self.authenticated:
                self.autenticar()

        # ESTADO -1 - erro
        # ESTADO 0 - não em execução
        # ESTADO 99 - finalizado

        # ESTADO 1 - realiza nova autenticação do bot a cada 30 min
        elif self.state == 1:
            if time.time()-self.auth_time > 1800:
                self.authenticated = False
                print("Passaram 30min, fazendo nova autenticacao para o Worker ", self.id)
            else:
                #avança para o próximo estado
                self.state = 2
        
        # ESTADO 2 - consulta o CPF
        elif self.state == 2:
            self.working_paciente_json = self.vacivida.consultacpf(self.working_entry['NUM_CPF'])
            cadastro_status = self.vacivida.get_consult_message()
            print(cadastro_status)

            if ("Usuário NÃO Cadastrado" in cadastro_status) :
                self.state = 3
            elif ("Usuário Cadastrado") in cadastro_status :
                self.state = 5
            elif ("Token inválido" in cadastro_status):
                # define como não autenticado e não altera o estado
                self.authenticated = False
            else:
                self.state = -1    

        # ESTADO 3 - inicia loop p/. tentar cadastrar paciente
        elif self.state == 3:
            db.update("age_agendamento_covid", "ind_vacivida_cadastro", "F", "SEQ_AGENDA",self.working_entry["SEQ_AGENDA"])
            db.update("age_agendamento_covid", "ind_vacivida_vacinacao", "F", "SEQ_AGENDA",self.working_entry["SEQ_AGENDA"])
            
            self.remaining_retry = MAX_RETRY -1
            self.state = 4

        # ESTADO 4 - loop de cadastro do paciente
        elif self.state == 4:            
            self.working_paciente_json, cadastrar_status = self.vacivida.cadastrar_paciente(self.working_entry)
            #cadastrar_status = self.vacivida.get_cadastro_message()
            print(cadastrar_status)

            if ("Incluído com Sucesso" in cadastrar_status) :
                db.update("age_agendamento_covid", "ind_vacivida_cadastro", "T", "SEQ_AGENDA",self.working_entry["SEQ_AGENDA"])
                #avança p/ próximo estado
                self.state = 8       
            elif self.remaining_retry > 0:
                #se mantém no mesmo estado até alcançar MAX_RETRY
                print("Tentativas restantes: ", self.remaining_retry)
                self.remaining_retry = self.remaining_retry - 1
                
                # verifica o erro
                # erro de token inválido: define como não autenticado
                if ("Token inválido." in cadastrar_status):
                    self.authenticated = False                
                # erro de formatação do CNS: descarta o CNS
                elif ("O CNS do paciente é obrigatório e deve conter 15 dígitos" in cadastrar_status):
                    self.working_entry['NUM_CNS'] = None
            else:
                #finaliza com erro quando atinge MAX_RETRY tentativas
                self.state = -1


        # ESTADO 5 - verifica necessidade de atualizar o cadastro do paciente
        elif self.state == 5:
            if self.working_paciente_json["CodigoSexo"] == None:     #pré-cadastro do vacivida apresenda dados inconsistentes e precisa ser atualizado
                print("Necessário atualizar o paciente", self.working_entry['NUM_CPF'])
                self.state = 6
            else:
                self.state = 8
                
        # ESTADO 6 - inicia loop p/. tentar atualizar paciente
        elif self.state == 6:
            self.remaining_retry = MAX_RETRY -1
            self.state = 7

        # ESTADO 7 - loop de atualização do paciente
        elif self.state == 7:           
            self.working_paciente_json, atualizacao_message = self.vacivida.atualizar_paciente(self.working_entry, self.working_paciente_json )                
            print(atualizacao_message)

            if ("atualizado" in atualizacao_message) :
                db.update("age_agendamento_covid", "ind_vacivida_cadastro", "T", "SEQ_AGENDA",self.working_entry["SEQ_AGENDA"])
                #avança
                self.state = 8
            elif self.remaining_retry > 0:
                #se mantém no mesmo estado até alcançar MAX_RETRY
                print("Tentativas restantes: ", self.remaining_retry)
                self.remaining_retry = self.remaining_retry - 1

                #verifica se o erro foi de token inválido e define como não autenticado
                if ("Token inválido." in atualizacao_message):
                    self.authenticated = False    
                # erro de formatação do CNS: descarta o CNS
                elif ("O CNS do paciente é obrigatório e deve conter 15 dígitos" in atualizacao_message):
                    self.working_entry['NUM_CNS'] = None
            else:
                #finaliza com erro quando atinge MAX_RETRY tentativas
                self.state = -1

        # ESTADO 8 - inicia loop p/. tentar cadastrar imunização
        elif self.state == 8:
            self.working_entry['ID_PACIENTE'] = self.working_paciente_json["IdPaciente"]

            self.remaining_retry = MAX_RETRY -1
            self.state = 9
            
        # ESTADO 9 - loop de cadastro de imunização
        elif self.state == 9:
            self.vacivida.imunizar(self.working_entry)
            imunizar_status = self.vacivida.get_imunizar_message()
            print(imunizar_status)

            if ("Incluído com Sucesso" in imunizar_status) :
                self.state = 10
            elif ("já tomou esta dose" in imunizar_status) :
                self.state = 10
            elif ("Não é permitido que a 2ª dose da vacina seja diferente da 1ª dose CPF" in imunizar_status) :
                self.state = 11
            elif ("A primeira dose do paciente não está registrado no VaciVida." in imunizar_status) :
                self.state = 12
            elif ("Não é permitido vacinar paciente menor de 18 anos de idade" in imunizar_status) :
                self.state = 13
            elif ("Paciente não possui 1a dose válida." in imunizar_status) :
                self.state = 15
            elif ("Erro" in imunizar_status) :
                self.state = 14

            elif self.remaining_retry > 0:
                #se mantém no mesmo estado até alcançar MAX_RETRY
                print("Tentativas restantes: ", self.remaining_retry)
                self.remaining_retry = self.remaining_retry - 1
                
                #verifica se o erro foi de token inválido e define como não autenticado
                if ("Token inválido." in imunizar_status):
                    self.authenticated = False
            else:
                #finaliza com erro quando atinge MAX_RETRY tentativas
                self.state = -1
                
        # ESTADO 10 - imunização registrada com sucesso
        elif self.state == 10:
            db.update("age_agendamento_covid", "ind_vacivida_vacinacao", "T", "SEQ_AGENDA",self.working_entry["SEQ_AGENDA"])                   
            print("Vacinacao SEQ_AGENDA = ", self.working_entry['SEQ_AGENDA'], " atualizado para True")
            self.state = 99

        # ESTADO 11 - erro no registro de imunização - 2a dose diferente da 1a
        elif self.state == 11:
            db.update("age_agendamento_covid", "ind_vacivida_vacinacao", "E", "SEQ_AGENDA",self.working_entry["SEQ_AGENDA"])                   
            print("Vacinacao SEQ_AGENDA = ", self.working_entry['SEQ_AGENDA'], " atualizado para Erro")
            self.state = 99

        # ESTADO 12 - erro no registro de imunização - 2a dose sem a 1a cadastrada no VaciVida
        elif self.state == 12:
            db.update("age_agendamento_covid", "ind_vacivida_vacinacao", "I", "SEQ_AGENDA",self.working_entry["SEQ_AGENDA"])                   
            print("Vacinacao SEQ_AGENDA = ", self.working_entry['SEQ_AGENDA'], " atualizado para Inconsistente")
            self.state = 99

        # ESTADO 13 - erro no registro de imunização - data de nascimento incorreta
        elif self.state == 13:
            db.update("age_agendamento_covid", "ind_vacivida_vacinacao", "X", "SEQ_AGENDA",self.working_entry["SEQ_AGENDA"])                   
            print("Vacinacao SEQ_AGENDA = ", self.working_entry['SEQ_AGENDA'], " atualizado para Data de Nascimento Incorreto")
            self.state = 99

        # ESTADO 14 - erro no registro de imunização - outros
        elif self.state == 14:
            db.update("age_agendamento_covid", "ind_vacivida_vacinacao", "Y", "SEQ_AGENDA",self.working_entry["SEQ_AGENDA"])                   
            print("Vacinacao SEQ_AGENDA = ", self.working_entry['SEQ_AGENDA'], " atualizado para Outros Erros")
            self.state = 99
            
        # ESTADO 15 - erro no registro de imunização - 1a dose não inserida
        elif self.state == 15:
            # atualiza todos os registros do paciente p/ Falso, caso tenha ocorrido algum erro na inserção da 1a dose
            db.update("age_agendamento_covid", "ind_vacivida_vacinacao", "F", "NUM_CPF",self.working_entry["NUM_CPF"])                   
            print("Vacinacoes de NUM_CPF = ", self.working_entry['NUM_CPF'], " atualizado para Falso")
            self.state = 99
            
        else:
            print("FATAL - Erro no worker")
            self.state = -1

    def autenticar(self):
        auth_message = self.vacivida.autenticar(self.login)        
        print("Worker ", self.id, auth_message)
        self.authenticated = (auth_message == "Autenticado!")
        if self.authenticated:
            self.auth_time = time.time()
        else:
            time.sleep(5)

class Supervisor():
    def __init__(self, area, cadastros, login, run=False):
        self.area = area
        self.queue = cadastros
        self.login = login
        self.fillers = []

        print("Inicializando supervisor de ", self.area)

    async def run(self, n_workers):
        Filler_remote = ray.remote(Filler)

        while (True):
            if len(self.fillers) < n_workers:
                # inicializa os actors Filler
                #Filler(0,registers_to_send, login_vacivida[alias], run=True)        #debug
                handles = [Filler_remote.remote(j, self.area, self.pop_entries(), self.login, run=False) for j in range(n_workers-len(self.fillers))]
                [h.run.remote() for h in handles]

                # agrega os novos Actor_handles de Filler_remote à lista para manter a referência e manter os Actors vivos
                self.fillers += handles

            await asyncio.sleep(DISPATCHER_WAIT)

    def pop_entries(self, n=WORKING_QUEUE):
        entries = []
        [ entries.append(self.queue.pop(0)) for _ in range( min(n, len(self.queue)) )  ]
        return entries

