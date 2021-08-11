import os
import sys
import inspect

currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(currentdir)
sys.path.insert(0, parentdir) 

import csv
from settings import db
from datetime import datetime

with open('scripts/lotes_desativados.csv') as csv_file:
    csv_reader = csv.reader(csv_file, delimiter=',')
    line_count = 0
    for row in csv_reader:        
        try:
            db.update("AGE_LOTE_VACINA_COVID", "IND_ATIVO", "S", "DSC_TIPO_VACINA", row[0], "=", "NUM_LOTE_VACINA", row[1], "=")
            print("Lote habilitado no Banco de Dados: ", row[0], row[1])
            #salva lotes p/ consulta
            with open("scripts/lotes_reativados.csv", "a") as fp:
                fp.write(f"{row[0]},{row[1]},{datetime.now()}\n")
        except:
            print("Lote NÃO habilitado no Banco de Dados: ", row[0], row[1])
            #salva lotes p/ consulta
            with open("scripts/lotes_nao_reativados.csv", "a") as fp:
                fp.write(f"{row[0]},{row[1]},{datetime.now()}\n")        

        line_count += 1
    print(f'Processed {line_count} lines.')