import os
import sys
import inspect

currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(currentdir)
sys.path.append(parentdir)

json_file = "vacibot/scripts/vacinadores.json"
with open(json_file, "w") as fp:
    fp.write("[\n")

from vacivida import Vacivida_Sys as vs
from credentials import login_vacivida as login
import local_dicts as di
from time import sleep
import json

v = vs()
vacinadores = {}
for unidade in login:
    #v.autenticar(login[unidade])
    print(unidade, v.autenticar(login[unidade]) )
    vacinadores[unidade] = v.get_vacinadores(di.estabelecimento[unidade])
    print(json.dumps(vacinadores[unidade], indent=4))

#print (json.dumps(vacinadores, indent=4))
with open(json_file , "w") as fp:
    json.dump(vacinadores, fp, indent=4, ensure_ascii=False)