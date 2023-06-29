import transactions
import objetos
from objetos import Objetos

def lock_read(vetor):
    bloqueio = ['RL']
    t = vetor[1].get_transaction()
    bloqueio.append(t)
    vetor[2].bloqueios.append(bloqueio)
    ordem = list(vetor[2].parentes.keys())
    ordem = ordem[:vetor[2].index][::-1]
    for i in ordem:
        bloqueio = ['IRL']
        new = vetor[1].get_transaction()
        bloqueio.append(t)
        vetor[2].parentes[i][0].bloqueios.append(bloqueio)
    ordem = list(vetor[2].parentes.keys())
    ordem = ordem[vetor[2].index+1:]
    for j in ordem:
        bloqueio = ['RL']
        new = vetor[1].get_transaction()
        bloqueio.append(t)
        vetor[2].parentes[j][0].bloqueios.append(bloqueio)

def lock_write(vetor):
    bloqueio = ['WL']
    t = vetor[1].get_transaction()
    bloqueio.append(t)
    vetor[2].bloqueios.append(bloqueio)
    ordem = list(vetor[2].parentes.keys())
    ordem = ordem[:vetor[2].index][::-1]
    for i in ordem:
        bloqueio = ['IWL']
        new = vetor[1].get_transaction()
        bloqueio.append(t)
        vetor[2].parentes[i][0].bloqueios.append(bloqueio)
    ordem = list(vetor[2].parentes.keys())
    ordem = ordem[vetor[2].index+1:]
    for j in ordem:
        bloqueio = ['WL']
        new = vetor[1].get_transaction()
        bloqueio.append(t)
        vetor[2].parentes[j][0].bloqueios.append(bloqueio)

def lock_update(vetor):
    bloqueio = ['UL']
    t = vetor[1].get_transaction()
    bloqueio.append(t)
    vetor[2].bloqueios.append(bloqueio)
    ordem = list(vetor[2].parentes.keys())
    ordem = ordem[:vetor[2].index][::-1]
    for i in ordem:
        bloqueio = ['IUL']
        new = vetor[1].get_transaction()
        bloqueio.append(t)
        vetor[2].parentes[i][0].bloqueios.append(bloqueio)
    ordem = list(vetor[2].parentes.keys())
    ordem = ordem[vetor[2].index+1:]
    for j in ordem:
        bloqueio = ['UL']
        new = vetor[1].get_transaction()
        bloqueio.append(t)
        vetor[2].parentes[j][0].bloqueios.append(bloqueio)

def liberar_locks(objeto, transaction):
    verifica = transaction.get_transaction()
    for i, j in reversed(list(enumerate(objeto.bloqueios))):
        if j[1] == verifica:
            del objeto.bloqueios[i]
    ordem = list(objeto.parentes.keys())
    ordem = ordem[:objeto.index][::-1]
    for i in ordem:
        for j, k in reversed(list(enumerate(objeto.parentes[i][0].bloqueios))):
            if k[1] == verifica:
                del objeto.parentes[i][0].bloqueios[j]
    ordem = list(objeto.parentes.keys())
    ordem = ordem[objeto.index+1:]
    for i in ordem:
        for j, k in reversed(list(enumerate(objeto.parentes[i][0].bloqueios))):
            if k[1] == verifica:
                del objeto.parentes[i][0].bloqueios[j]

def lock_certify(objeto, transaction):
    transaction = transaction.get_transaction()
    for i, j in enumerate(objeto.bloqueios):
        if j[1] == transaction and j[0] == 'WL':
            objeto.bloqueios[i][0] = 'CL'
    ordem = list(objeto.parentes.keys())
    ordem = ordem[:objeto.index][::-1]
    for i in ordem:
        for j, k in enumerate(objeto.parentes[i][0].bloqueios):
            if k[1] == transaction and k[0] == 'IWL':
                objeto.parentes[i][0].bloqueios[j][0] = 'ICL'
    ordem = list(objeto.parentes.keys())
    ordem = ordem[objeto.index+1:]
    for i in ordem:
        for j, k in enumerate(objeto.parentes[i][0].bloqueios):
            if k[1] == transaction and k[0] == 'IWL':
                objeto.parentes[i][0].bloqueios[j][0] = 'ICL'


def check_locks(vetor, tipo:str, transaction) -> tuple:
    transactions = []
    if len(vetor) == 0: return (True, None)    
    if tipo == 'RL':
        for i in vetor[0][2].bloqueios:
            if i[0] == 'CL' or i[0] == 'ICL':
                if i[1] != transaction.get_transaction():
                    return (False, i[1])
            transactions.append(i[1])
        return (True, transactions)
    else:
        for i in vetor[0][2].bloqueios:
            if i[0] == 'CL' or i[0] == 'WL' or i[0] == 'ICL' or i[0] == 'IWL':
                if i[1] != transaction.get_transaction(): 
                    return (False, i[1])
            transactions.append(i[1])
        return (True, transactions)
    return (True, None)
    

def converte_certify(vetor, transaction):
    vetor_obj = []
    for i in vetor:
        if i[1].get_transaction() == transaction.get_transaction():
            vetor_obj.append(i[2])
    vetor_obj_2 = []
    for j in vetor_obj:
        selection = True
        for k in j.bloqueios:
            if k[0] == 'RL' and k[1] != transaction.get_transaction():
                selection = False
                break
        if selection:
            vetor_obj_2.append(j)       
    for d in vetor_obj_2:
        lock_certify(d, transaction)

        
                

