import time, logging, ray, asyncio
from vacivida import Vacivida_Sys
from settings import db, MAX_RETRY, WORKING_QUEUE, DISPATCHER_WAIT, MAX_WORKERS
import dicts as di

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

        self._print(f"Inicializando worker")

        if run:
            self.run()


    def run(self):
        while True:        # roda enquanto houver cadastros não inseridos no vacivida
            if not self.cadastros_worker:
                # se a lista de cadastros está vazia, solicita mais cadastros ao Supervisor
                self._print(f"Requisitando novos cadastros ao supervisor")
                supervisor = ray.get_actor(f"{self.area}.supervisor")
                new_entries = ray.get( supervisor.pop_entries.remote() )
                self.cadastros_worker.extend(new_entries)

                # se o supervisor não devolver novos cadastros, sai do loop
                if not self.cadastros_worker:
                    self._print("Lista de cadastros finalizada")
                    supervisor.notify.remote(self.id)
                    break

            # inicia o processo de cadastro
            self.working_entry = self.cadastros_worker.pop(0)
            self.state = 1
            self.working_paciente_json = None
            self.remaining_retry = 0
            self.error_message = ""
            self.error_state = 0
            while True:
                # condições de parada
                if  self.state == 99:
                    self._print(f"[SEQ_AGENDA={self.working_entry['SEQ_AGENDA']}] Finalizado com Sucesso")
                    break
                elif self.state == -1:
                    self._print(f"[SEQ_AGENDA={self.working_entry['SEQ_AGENDA']}] Finalizado com Erro: {self.error_message}")
                    break
                elif self.state == -2:
                    self._print(f"[SEQ_AGENDA={self.working_entry['SEQ_AGENDA']}] Finalizado com Erro Tratado: {self.error_message}")
                    break

                # execução
                try:
                    self.step()
                    #self._print(self.id,self.vacivida.login_token[-44:])     #debug
                except Exception as e:
                    self.state = -1
                    self.error_message = "CRITICAL - Um Exception lançado no processamento do cadastro."
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
                self._print("Passaram 30min, fazendo nova autenticacao para o Worker")
            else:
                #avança para o próximo estado
                self.state = 2
        
        # ESTADO 2 - consulta o CPF
        elif self.state == 2:
            self.working_paciente_json = self.vacivida.consultacpf(self.working_entry['NUM_CPF'])
            cadastro_status = self.vacivida.get_consult_message()
            #self._print(cadastro_status)

            if ("Usuário NÃO Cadastrado" in cadastro_status) :
                self.state = 3
            elif ("Usuário Cadastrado") in cadastro_status :
                self.state = 5
            elif ("Token inválido" in cadastro_status):
                # define como não autenticado e não altera o estado
                self.authenticated = False
            else:
                # atribui parametros do erro e avança para o estado de erro
                self.error_state = self.state
                self.error_message = cadastro_status
                self.state = -1

        # ESTADO 3 - inicia loop p/. tentar cadastrar paciente
        elif self.state == 3:
            #db.update("age_agendamento_covid", "ind_vacivida_cadastro", "F", "SEQ_AGENDA",self.working_entry["SEQ_AGENDA"])
            #db.update("age_agendamento_covid", "ind_vacivida_vacinacao", "F", "SEQ_AGENDA",self.working_entry["SEQ_AGENDA"])
            
            self.remaining_retry = MAX_RETRY -1
            self.state = 4

        # ESTADO 4 - loop de cadastro do paciente
        elif self.state == 4:            
            self.working_paciente_json, cadastrar_status = self.vacivida.cadastrar_paciente(self.working_entry)
            #cadastrar_status = self.vacivida.get_cadastro_message()
            #self._print(cadastrar_status)

            if ("Incluído com Sucesso" in cadastrar_status) :
                db.update("age_agendamento_covid", "ind_vacivida_cadastro", "T", "SEQ_AGENDA",self.working_entry["SEQ_AGENDA"])
                #avança p/ próximo estado
                self.state = 8       
            elif self.remaining_retry > 0:
                #se mantém no mesmo estado até alcançar MAX_RETRY
                #self._print("Tentativas restantes: ", self.remaining_retry)
                self.remaining_retry = self.remaining_retry - 1
                
                # verifica o erro
                self.check_record_error(cadastrar_status)
            else:
                #finaliza com erro quando atinge MAX_RETRY tentativas
                # atribui parametros do erro e avança para o estado de erro
                self.error_state = self.state
                self.error_message = cadastrar_status
                self.state = -1

        # ESTADO 5 - verifica necessidade de atualizar o cadastro do paciente
        elif self.state == 5:
            if   self.working_entry["IND_VACIVIDA_CADASTRO"] == "U":
                # recebe flag de update do banco
                self.state = 6
            elif self.working_paciente_json["CodigoSexo"] == None:
                # se o pré-cadastro do vacivida apresenda dados inconsistentes e precisa ser atualizado
                #self._print("Necessário atualizar o paciente", self.working_entry['NUM_CPF'])
                db.update("age_agendamento_covid", "ind_vacivida_cadastro", "U", "SEQ_AGENDA",self.working_entry["SEQ_AGENDA"])
                self.state = 6
            else:
                if self.working_entry["IND_VACIVIDA_CADASTRO"] != "T":
                    # atualiza o IND_VACIVIDA_CADASTRO para True
                    db.update("age_agendamento_covid", "ind_vacivida_cadastro", "T", "SEQ_AGENDA",self.working_entry["SEQ_AGENDA"])
                self.state = 8
                
        # ESTADO 6 - inicia loop p/. tentar atualizar paciente
        elif self.state == 6:
            self.remaining_retry = MAX_RETRY -1
            self.state = 7

        # ESTADO 7 - loop de atualização do paciente
        elif self.state == 7:           
            paciente_json, atualizacao_message = self.vacivida.atualizar_paciente(self.working_entry, self.working_paciente_json )                
            #self._print(atualizacao_message)

            if ("atualizado" in atualizacao_message) :
                self.working_paciente_json = paciente_json
                db.update("age_agendamento_covid", "ind_vacivida_cadastro", "T", "SEQ_AGENDA",self.working_entry["SEQ_AGENDA"])
                #avança
                self.state = 8
            elif self.remaining_retry > 0:
                #se mantém no mesmo estado até alcançar MAX_RETRY
                #self._print("Tentativas restantes: ", self.remaining_retry)
                self.remaining_retry = self.remaining_retry - 1
                
                # verifica o erro
                self.check_record_error(atualizacao_message)
            else:
                #finaliza com erro quando atinge MAX_RETRY tentativas
                # atribui parametros do erro e avança para o estado de erro
                self.error_state = self.state
                self.error_message = atualizacao_message
                self.state = -1

        # ESTADO 8 - verifica o cadastro de imunização
        elif self.state == 8:
            historico = self.vacivida.get_historico_vacinacao( self.working_paciente_json["IdPaciente"] )

            # verifica se o paciente ainda não foi vacinado no Vacivida
            on_vacivida = 0
            for vacinacao in historico:
                if vacinacao["IdDose"] == self.working_entry["NUM_DOSE_VACINA"]:
                    # vacinação já inserida no vacivida
                    on_vacivida += 1
            
            if on_vacivida == 1:
                # já está inserido no vacivida - avança p/ estado de imunização bem sucedida
                self.state = 10

            elif on_vacivida > 1:           
                # dose duplicada - avança p/ erro tratado
                self.state = 16

            else:
                # vacinação não inserida no vacivida
                if self.working_entry['IND_VACIVIDA_VACINACAO'] == "T":
                    # não está inserido no vacivida e o bd aponta como inserido
                    # atualiza bd
                    db.update("age_agendamento_covid", "IND_VACIVIDA_VACINACAO", "F", "SEQ_AGENDA",self.working_entry["SEQ_AGENDA"])

                if self.working_entry["DSC_PUBLICO"] == di.grupo_id["COMORBIDADE"] and self.working_entry["NUM_DOSE_VACINA"] == di.dose_id['2']:
                    # 2a dose do público de comorbidades precisa utilizar o mesmo CRM
                    for vacinacao in historico:
                        if vacinacao["IdDose"] == di.dose_id["1"]:
                            self.working_entry["NUM_CRM"] = vacinacao["CRMComorbidade"]
                 
                # avança para o próximo estado
                self.state = 8.1

        # ESTADO 8.1 - inicia loop p/. tentar cadastrar imunização
        elif self.state == 8.1:
            self.working_entry['ID_PACIENTE'] = self.working_paciente_json["IdPaciente"]

            self.remaining_retry = MAX_RETRY -1
            self.state = 9

        # ESTADO 9 - loop de cadastro de imunização
        elif self.state == 9:
            self.vacivida.imunizar(self.working_entry)
            imunizar_status = self.vacivida.get_imunizar_message()
            #self._print(imunizar_status)

            if ("Incluído com Sucesso" in imunizar_status) :
                self.state = 10
            elif ("já tomou esta dose" in imunizar_status) :
                self.state = 10
            elif "Já existe uma primeira dose para o esquema vacinal atual" in imunizar_status :
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
                #self._print("Tentativas restantes: ", self.remaining_retry)
                self.remaining_retry = self.remaining_retry - 1
                
                #verifica se o erro foi de token inválido e define como não autenticado
                if ("Token inválido." in imunizar_status):
                    self.authenticated = False
            else:
                #finaliza com erro quando atinge MAX_RETRY tentativas
                # atribui parametros do erro e avança para o estado de erro
                self.error_state = self.state
                self.error_message = imunizar_status
                self.state = -1
                
        # ESTADO 10 - imunização registrada com sucesso // imunização já registrada
        elif self.state == 10:
            if self.working_entry['IND_VACIVIDA_VACINACAO'] != "T":
                db.update("age_agendamento_covid", "IND_VACIVIDA_VACINACAO", "T", "SEQ_AGENDA",self.working_entry["SEQ_AGENDA"])
            self.state = 99

        # ESTADO 11 - erro no registro de imunização - 2a dose diferente da 1a
        elif self.state == 11:
            db.update("age_agendamento_covid", "ind_vacivida_vacinacao", "E", "SEQ_AGENDA",self.working_entry["SEQ_AGENDA"])                   
            self.error_message = f"Vacinacao SEQ_AGENDA = {self.working_entry['SEQ_AGENDA']} atualizado para Erro"
            self.state = -2

        # ESTADO 12 - erro no registro de imunização - 2a dose sem a 1a cadastrada no VaciVida
        elif self.state == 12:
            db.update("age_agendamento_covid", "ind_vacivida_vacinacao", "I", "SEQ_AGENDA",self.working_entry["SEQ_AGENDA"])                   
            self.error_message = f"Vacinacao SEQ_AGENDA = {self.working_entry['SEQ_AGENDA']} atualizado para Inconsistente"
            self.state = -2

        # ESTADO 13 - erro no registro de imunização - data de nascimento incorreta
        elif self.state == 13:
            db.update("age_agendamento_covid", "ind_vacivida_vacinacao", "X", "SEQ_AGENDA",self.working_entry["SEQ_AGENDA"])                   
            self.error_message = f"Vacinacao SEQ_AGENDA = {self.working_entry['SEQ_AGENDA']} atualizado para Data de Nascimento Incorreto"
            self.state = -2

        # ESTADO 14 - erro no registro de imunização - outros
        elif self.state == 14:
            db.update("age_agendamento_covid", "ind_vacivida_vacinacao", "Y", "SEQ_AGENDA",self.working_entry["SEQ_AGENDA"])                   
            self.error_message = f"Vacinacao SEQ_AGENDA = {self.working_entry['SEQ_AGENDA']} atualizado para Outros Erros"
            self.state = -2

        # ESTADO 15 - erro no registro de imunização - 1a dose inválida
        elif self.state == 15:
            tag = "P"
            if self.working_entry['IND_VACIVIDA_VACINACAO'] != tag:
                db.update("age_agendamento_covid", "IND_VACIVIDA_VACINACAO",tag, "SEQ_AGENDA",self.working_entry["SEQ_AGENDA"])            
            self.error_message = f"Atualizado p/ P - 1a dose inválida"
            self.state = -2
            
        # ESTADO 16 - imunização duplicada no histórico de imunização do vacivida
        elif self.state == 16:
            tag = "D"
            if self.working_entry['IND_VACIVIDA_VACINACAO'] != tag:
                db.update("age_agendamento_covid", "IND_VACIVIDA_VACINACAO",tag, "SEQ_AGENDA",self.working_entry["SEQ_AGENDA"])
            self.error_message = f"Atualizado p/ D - Imunização duplicada no Vacivida"
            self.state = -2
            
        else:
            self.error_message = "CRITICAL - Estado inválido: "+str(self.state)
            self.error_state = self.state
            self.state = -1

    def autenticar(self):
        auth_message = self.vacivida.autenticar(self.login)        
        self._print(auth_message)
        self.authenticated = (auth_message == "Autenticado!")
        if self.authenticated:
            self.auth_time = time.time()
        else:
            time.sleep(5)
    
    def check_record_error(self, error_message):
        # erro de token inválido: define como não autenticado
        if ("Token inválido." in error_message):
            self.authenticated = False                
        # erro de formatação do CNS: descarta o CNS
        elif ("O CNS do paciente é obrigatório e deve conter 15 dígitos" in error_message):
            self.working_entry['NUM_CNS'] = None
        # erro de CNS duplicado: descarta o CNS
        elif ("CNS do paciente já cadastrado" in error_message):
            self.working_entry['NUM_CNS'] = None

    def _print(self, *args):
        print(f"[{self.area[0:14]:^15}|{('filler-'+str(self.id)):^10}]", *args)

class Supervisor():
    def __init__(self, area, cadastros, login, run=False, n_workers=0):
        self.area = area
        self.queue = cadastros
        self.login = login
        self.fillers = []
        self.last_filler = -1
        self.n_workers = n_workers

        self._print(f"Inicializando supervisor com {len(self.queue)} cadastros")

    async def run(self, n_workers=None):
        if n_workers:
            self.n_workers = n_workers
        Filler_remote = ray.remote(Filler)

        while (self.queue):
            # enquanto houver cadastros na fila
            # dispacha um Filler por vez e espera DISPATCHER_WAIT segundos
            if len(self.fillers) < self.n_workers:
                # inicializa novo actor Filler
                self.last_filler += 1
                f_id   = self.last_filler
                f_name = f'{self.area}.filler-{f_id}'
                filler = Filler_remote.options(name=f_name).remote(f_id, self.area, self.pop_entries(), self.login, run=False)

                filler.run.remote()

                # agrega o novo Actor_handle de Filler_remote à lista para manter a referência e manter o Actor vivo
                self.fillers.append(filler)

            await asyncio.sleep(DISPATCHER_WAIT)

        # fila finalizada
        # aguarda fillers notificarem término
        while (self.fillers):
            await asyncio.sleep(5)
        
        self._print("Finalizada execução")
        # notifica Manager
        manager = ray.get_actor("manager")
        manager.notify.remote(self.area)

    def pop_entries(self, n=WORKING_QUEUE):
        entries = []
        [ entries.append(self.queue.pop(0)) for _ in range( min(n, len(self.queue)) )  ]
        return entries

    def _print(self, str):
        print(f"[{self.area[0:14]:^15}|supervisor] {str}")

    def set_n_workers(self, n):
        self.n_workers = n
    def get_n_workers(self):
        return self.n_workers
    def add_n_workers(self, n):
        self.n_workers += n

    def notify(self, filler_id, cadastros=[]):
        # retorna os cadastros remanescentes à fila
        if cadastros:
            self.cadastros.append(cadastros)        
        # encontra o handler do filler
        filler = ray.get_actor(f"{self.area}.filler-{filler_id}")        
        # termina o filler
        ray.kill(filler)
        # encontra o handler local do filler e remove da lista
        for h in self.fillers:
            if h._ray_actor_id == filler._ray_actor_id:
                self.fillers.remove(h)
                break

class Manager:
    def __init__(self, max_workers=MAX_WORKERS):
        self.max_workers = max_workers
        self.used_workers = 0
        self.supervisors = {}

    def create_supervisor(self, area, records, login):
        h = ray.remote(Supervisor).options(name=f"{area}.supervisor").remote(area, records, login)
        self.supervisors[area] = {
            "handler":h,
            "n_workers":0,
            "list_size":len(records)
        }
        return True

    async def run(self):
        # verifica se os supervisores foram inicializados
        if not self.supervisors:
            self._print("Nenhum supervisor foi inicializado. É necessário inicializar os supervisores antes de executar.")
            return False

        # inicializa cada supervisor
        for area in self.supervisors:
            s = self.supervisors[area]
            while True:
                if self.used_workers <= self.max_workers:
                    self.used_workers += 1
                    s["n_workers"] = 1
                    s["handler"].run.remote(1)
                    break
                else:
                    await asyncio.sleep(DISPATCHER_WAIT)
                    #time.sleep(DISPATCHER_WAIT)        
        self._print("Todos os supervisores foram iniciados com 1 worker")

        # enquanto houverem supervisores ativos, verifica se
        # existem workers disponíveis e os distribui p/ cada supervisor
        while self.supervisors:
            if self.used_workers <= self.max_workers:
                # encontra o supervisor com menor n_workers
                min = self.max_workers
                for area in self.supervisors:
                    if self.supervisors[area]["n_workers"] < min:
                        min = self.supervisors[area]["n_workers"] 
                        s = self.supervisors[area]
                        a = area
                del min

                # acrescenta 1 n_worker
                self.used_workers += 1
                s["n_workers"] += 1
                s["handler"].add_n_workers.remote(1)
                self._print(f"novo worker atribuido ao {a}")

            else:
                await asyncio.sleep(DISPATCHER_WAIT)
                #time.sleep(DISPATCHER_WAIT)

        self._print("Execução finalizada")
        return True

    def notify(self, area):
        # encontra o handler do supervisor
        supervisor = ray.get_actor(f"{area}.supervisor")   

        # termina o supervisor
        ray.kill(supervisor)
        # encontra o handler local
        for area in self.supervisors:
            if self.supervisors[area]["handler"]._ray_actor_id == supervisor._ray_actor_id:
                # retorna workers para serem reutilizados
                n = self.supervisors[area]["n_workers"]           
                self.used_workers -= n
                self._print(f"Supervisor de {area} devolveu {n} workers")
                self.supervisors.pop(area)
                break

    def _print(self, str):
        print(f"[{'manager':^26}] {str}")


