import os
import sys
import inspect

currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(currentdir)
sys.path.insert(0, parentdir) 

from settings import db
from datetime import datetime
from vacivida import Vacivida_Sys
from credentials import login_vacivida
import dicts as di

csv_file = "vacibot/scripts/vacinacoes_excluidas.csv"
with open(csv_file, "a") as fp: 
    pass

header, rows = db.fetch("vacibot_select", "IND_VACIVIDA_VACINACAO", "I")
records = [ { header[i]:row[i] for i in range(len(header)) } for row in rows ]

vacivida = Vacivida_Sys()
vacivida.autenticar(list(login_vacivida.values())[0])

print ("Len:", len(records))
for record in records:
    #print(record)

    paciente_json = vacivida.consultacpf(record['NUM_CPF'])
    historico = vacivida.get_historico_vacinacao( paciente_json["IdPaciente"] )
    #print(json.dumps(historico, indent=4))

    vac_ids = []
    for vacinacao in historico:
        if vacinacao["IdDose"] == di.dose_id[str(record["NUM_DOSE_VACINA"])]:
            vac_ids.append(vacinacao["IdVacinacao"])
    vac_ids.pop(0)

    for vac_id in vac_ids:
        success, msg = vacivida.delete_vacinacao(vac_id)
        print( f"[SEQ_AGENDA={record['SEQ_AGENDA']} | IdVacinacao={vac_id}] {msg}" )
        with open(csv_file, "a") as fp:   
            # SEQ_AGENDA, IdVacinacao, IdPaciente, response_success, response_msg, datetime 
            fp.write(f"{record['SEQ_AGENDA']},{paciente_json['IdPaciente']},{vac_id},{success},{msg},{datetime.now()}\n")

    db.update("age_agendamento_covid", "IND_VACIVIDA_VACINACAO", "T", "SEQ_AGENDA",record["SEQ_AGENDA"])