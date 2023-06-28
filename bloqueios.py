import transactions
import objetos
from objetos import Objetos

"""
Função que concede um bloqueio de leitura sobre um determinado objeto do nosso esquema,
a função também concede bloqueios de intencional de leitura para os parentes que estão acima
do objeto em questão e também bloqueia para leitura objetos que estão abaixo do objeto referenciado.
"""
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

""""
Função que concede um bloqueio de escrita sobre um determinado objeto do nosso esquema,
a função também concede bloqueios de intencional de escrita para os parentes que estão acima
do objeto em questão e também bloqueia para escrita objetos que estão abaixo do objeto referenciado.
"""
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

"""
Função que libera bloqueios de uma transação sobre um determinado objeto e 
consequentemente os intencionais sobre os parentes desse objeto associado
a transação em questão. 
"""
def liberar_locks(objeto, transaction):
    verifica = transaction.get_transaction()
    for i, j in enumerate(objeto.bloqueios):
        if j[1] == verifica: del objeto.bloqueios[i]
    ordem = list(objeto.parentes.keys())
    ordem = ordem[:objeto.index][::-1]
    for i in ordem:
        for j, k in enumerate(objeto.parentes[i][0].bloqueios):
            if k[1] == verifica: del objeto.parentes[i][0].bloqueios[j]
    ordem = list(objeto.parentes.keys())
    ordem = ordem[objeto.index+1:]
    for i in ordem:
        for j, k in enumerate(objeto.parentes[i][0].bloqueios):
            if k[1] == verifica: del objeto.parentes[i][0].bloqueios[j]

"""
Função que converte bloqueios de escrita em bloqueios de certify associado
a uma determinada transação e também converte os intencionais de escrita em
intencionais de certify 
"""
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
    


