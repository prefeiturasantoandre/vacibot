import os
import sys
import inspect

currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(currentdir)
sys.path.append(parentdir)


import ray
from settings import db
from datetime import datetime
from vacivida import Vacivida_Sys
from credentials import login_vacivida_adm
import dicts as di

@ray.remote
class Queue:
    def __init__(self, records) -> None:
        self.q = records

    def pop(self):
        if self.q:
            return self.q.pop(0)
        return None

    def clear(self):
        self.q = []

@ray.remote
def worker(queue):    
    vacivida = Vacivida_Sys()
    vacivida.autenticar(login_vacivida_adm)
    
    while (record:=ray.get(queue.pop.remote())) :
        paciente_json = vacivida.consultacpf(record['NUM_CPF'])
        historico = vacivida.get_historico_vacinacao( paciente_json["IdPaciente"] )
        #print(json.dumps(historico, indent=4))

        vac_ids = []
        for vacinacao in historico:
            if vacinacao["IdDose"] == di.dose_id[str(record["NUM_DOSE_VACINA"])] and vacinacao["IdMunicipio"] == di.municipio :
                vac_ids.append(vacinacao["IdVacinacao"])
        if vac_ids:
            vac_ids.pop(0)

        succesful = True
        for vac_id in vac_ids:
            success, msg = vacivida.delete_vacinacao(vac_id)
            print( f"[SEQ_AGENDA={record['SEQ_AGENDA']} | IdVacinacao={vac_id}] {msg}" )
            with open(csv_file, "a") as fp:   
                # SEQ_AGENDA, IdVacinacao, IdPaciente, response_success, response_msg, datetime 
                fp.write(f"{record['SEQ_AGENDA']},{paciente_json['IdPaciente']},{vac_id},{success},{msg},{datetime.now()}\n")
                if not success: 
                    succesful = False

        if succesful:
            db.update("age_agendamento_covid", "IND_VACIVIDA_VACINACAO", "T", "SEQ_AGENDA",record["SEQ_AGENDA"])

if __name__ == "__main__":
    parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    os.environ["PYTHONPATH"] = parent_dir + ":" + os.environ.get("PYTHONPATH", "")
    ray.init()

    csv_file = "vacibot/scripts/vacinacoes_excluidas.csv"
    with open(csv_file, "a") as fp: 
        pass

    header, rows = db.fetch("vacibot_select", "IND_VACIVIDA_VACINACAO", "D")
    records = [ { header[i]:row[i] for i in range(len(header)) } for row in rows ]
    print ("Len:", len(records))

    #argument for n_workers
    try:
        n_workers = int(sys.argv[1])
    except:
        n_workers = 2

    queue = Queue.remote(records)
    workers = [ worker.remote(queue) for _ in range(n_workers) ]
    ray.get(workers)