import objetos
import bloqueios
import operations
import transactions
import main
import networkx as nx
import matplotlib.pyplot as plt
import copy

# Inicializando o banco de dados
ob = objetos.Objetos('Banco', 'BD')

# Esquema com 1 Banco de dados, 2 areas, cada área com 2 tabelas, cada tabela com 2 páginas e cada página com 2 tuplas.
dic = objetos.criar_esquema(ob,2,2,2,2)

scheduler = 'R2(TP1)R1(TP2)W2(TP3)R3(TP1)R1(TP3)W3(TP4)R2(TP4)W3(TP4)C3C1C2'

def cria_objetos(scheduler):
    elementos = list(scheduler)
    vetor_tran = []
    i = [i for i in range(len(elementos))]
    for j in i:
        if elementos[j] == 'R':
            vetor = []
            aux = []
            vetor.append(operations.Operation('R'))
            vetor.append(transactions.Transaction(elementos[j + 1]))
            aux.append(elementos[j + 3])
            aux.append(elementos[j + 4])
            aux.append(elementos[j + 5])
            vetor.append(dic[''.join(aux)])
            vetor_tran.append(vetor)
        if elementos[j] == 'W':
            vetor = []
            aux_1 = []
            vetor.append(operations.Operation('W'))
            vetor.append(transactions.Transaction(elementos[j + 1]))
            aux_1.append(elementos[j + 3])
            aux_1.append(elementos[j + 4])
            aux_1.append(elementos[j + 5])
            vetor.append(dic[''.join(aux_1)])
            vetor_tran.append(vetor)
        if elementos[j] == 'C':
            vetor = []
            vetor.append(operations.Operation('C')) 
            vetor.append(transactions.Transaction(elementos[j + 1]))
            vetor_tran.append(vetor)
    return vetor_tran

vetor_tran = cria_objetos(scheduler)

def cria_nos(grafo, vetor_tran):
    transactions = []
    for i in vetor_tran:
        if i[1] not in transactions:
            grafo.add_node(f'{i[1].get_transaction()}')
            transactions.append(i[1])

def grafo_espera(grafo):
    tem_ciclo = nx.is_directed_acyclic_graph(grafo)
    if tem_ciclo:
        return False
    else:
        return True

def verifica_escrita(transaction, objeto):
    for i in objeto.bloqueios:
        if transaction.get_transaction() == i[1] and i[0] == 'WL': return True

def verifica_leitura(vetor, transaction):
    vetor_obj = []
    for i in vetor:
        if i[1].get_transaction() == transaction.get_transaction() and i[0].get_operation() == 'Write':
            vetor_obj.append(i[2])
    for j in vetor_obj:
        for k in j.bloqueios:
            if k[0] == 'RL':
                if k[1] != transaction.get_transaction():
                    return (True, k[1])
    return (False, None)

def verifica_operation(vetor, transaction):
    for i in vetor:
        if i[1].get_transaction() == transaction.get_transaction(): return True

def locks_commit(vetor, transaction):
    for i in vetor:
        if i[0].operation != 'Commit':
            bloqueios.liberar_locks(i[2], transaction)

def abortar_transaction(vetor):
    transaction = vetor[-1][1].get_transaction()
    for k in reversed(range(len(vetor))):
        if vetor[k][1].get_transaction() == transaction:
            del vetor[k]
    return vetor

def converte_certify(vetor, transaction, k):
    vetor_obj = []    
    for i in vetor:
        if i[0].get_operation() != 'Commit':
            if i[1].get_transaction() == transaction.get_transaction() and i[0].get_operation() == 'Write':
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
        bloqueios.lock_certify(d, transaction)

def protocolo(vetor_tran):
    # Criar um objeto do tipo grafo direcionado, será o nosso grafo de espera
    grafo = nx.DiGraph()
    # Criar os nós do grafo
    cria_nos(grafo, vetor_tran)
    s = []
    while True:        
        esperando = []
        for k,i in enumerate(vetor_tran):
            if i[0].get_operation() == 'Write':
                analise, t = bloqueios.check_locks(s,i, 'WL', i[1])
                if analise != False: 
                    bloqueios.lock_write(i)                  
                    i[2].converte_version(i[1]) 
                    objeto_copy = copy.deepcopy(i)
                    s.append(objeto_copy)
                    i[2].version_normal()                                
                else:
                    grafo.add_edge(t,i[1].get_transaction())
                    if grafo_espera(grafo) == True: return 'O scheduler possui deadlock!!!!!!!'    
                    esperando.append(i)
            elif (i[0].get_operation() == 'Read'):
                analise, t = bloqueios.check_locks(s,i, 'RL', i[1])
                if analise != False:
                    bloqueios.lock_read(i)
                    if verifica_escrita(i[1], i[2]) == True:
                        i[2].converte_version(i[1]) 
                        objeto_copy = copy.deepcopy(i)
                        s.append(objeto_copy)
                        i[2].version_normal()
                    else:
                        s.append(i)
                else:
                    grafo.add_edge(t,i[1].get_transaction())
                    if grafo_espera(grafo) == True:
                        return 'O scheduler possui deadlock!!!!!!!'
                    esperando.append(i)          
            else:
                analise, t = verifica_leitura(s, i[1])               
                if analise == True:
                    converte_certify(s, i[1], k)
                    esperando.append(i) 
                    grafo.add_edge(t,i[1].get_transaction())
                    if grafo_espera(grafo) == True: return 'O scheduler possui deadlock!!!!!!!'
                elif verifica_operation(esperando, i[1]) == True:
                    if i not in esperando:
                        esperando.append(i)
                else:
                    locks_commit(vetor_tran, i[1])
                    locks_commit(s, i[1])
                    grafo.remove_node(i[1].get_transaction())
                    s.append(i)
        vetor_tran = esperando
        if len(vetor_tran) == 0: break
    return s

print(protocolo(vetor_tran))

"""
bloqueios.lock_read(vetor_tran[0])
bloqueios.lock_write(vetor_tran[1])
bloqueios.lock_write(vetor_tran[2])
converte_certify(vetor_tran, vetor_tran[3][1], 3)
"""

