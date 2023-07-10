import bloqueios
import operations
import transactions
import networkx as nx
import copy

# Cria os nós do nosso grafo de espera
def cria_nos(grafo, vetor_tran):
    transactions = []
    for i in vetor_tran:
        if i[1] not in transactions:
            grafo.add_node(f'{i[1].get_transaction()}')
            transactions.append(i[1])

# Verifica se o nosso grafo possui ciclo
def grafo_espera(grafo):
    tem_ciclo = nx.is_directed_acyclic_graph(grafo)
    if tem_ciclo:
        return False
    else:
        return True

# Verifica se há alguma outra transação escrevendo sobre um determinado objeto
def verifica_escrita(transaction, objeto):
    for i in objeto.bloqueios:
        if transaction.get_transaction() == i[1] and i[0] == 'WL': return True

# Verifica se há alguma outra transação lendo sobre um determinado objeto
def verifica_leitura(vetor, transaction):
    vetor_obj = []
    for i in vetor:
        if i[0].get_operation() != 'Commit' and i[0].get_operation() == 'Write':
            if i[1].get_transaction() == transaction.get_transaction():
                vetor_obj.append(i[2])
    for j in vetor_obj:
        for k in j.bloqueios:
            if k[0] == 'RL'or k[0] == 'IRL':
                if k[1] != transaction.get_transaction():
                    return (True, k[1])
    return (False, None)

# Função que vai garantir que uma transação vai ser executada corretamente
def verifica_operation(vetor, transaction):
    for i in vetor:
        if i[1].get_transaction() == transaction.get_transaction(): return True

# Função que vai liberar os bloqueios associados a uma determinada transação
def locks_commit(vetor, transaction):
    for i in vetor:
        if i[0].operation != 'Commit':
            bloqueios.liberar_locks(i[2], transaction)

# Função que aborta a transação mais recente quando o scheduler se envolve em deadlock
def abortar_transaction(vetor,vetor_2):
    trans = []
    for i in vetor:
        if i[1].get_transaction() not in trans:
            trans.append(i[1].get_transaction())
    transaction = trans[-1]
    for k in reversed(range(len(vetor_2))):
        if vetor_2[k][1].get_transaction() == transaction:
            del vetor_2[k]
    return vetor_2, transaction

# Função que vai converter os bloqueios de escrita em bloqueios de certify
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
            if k[0] == 'RL' or k[0] == 'IRL':
                if k[1] != transaction.get_transaction():
                    selection = False
                    break
        if selection:
            vetor_obj_2.append(j)
    for d in vetor_obj_2:
        bloqueios.lock_certify(d, transaction)

# Função que converte um bloqueio de update em escrita
def convert_update(objeto, transaction):
    transaction = transaction.get_transaction()
    for i, j in enumerate(objeto.bloqueios):
        if j[1] == transaction and j[0] == 'UL':
            objeto.bloqueios[i][0] = 'WL'

# Função principal que implementa o protocolo de controle de concorrência 2V2PL
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
                analise, t = bloqueios.check_locks(vetor_tran,i, 'WL', i[1])
                if analise != False: 
                    bloqueios.lock_write(i)                  
                    i[2].converte_version(i[1]) 
                    objeto_copy = copy.deepcopy(i)
                    s.append(objeto_copy)
                    i[2].version_normal()
                    convert_update(i[2], i[1])                               
                else:
                    grafo.add_edge(t,i[1].get_transaction())
                    if grafo_espera(grafo) == True:
                        novo_vetor_tran, transc = abortar_transaction(s, vetor_tran)
                        grafo.remove_node(transc)
                        return f"{transc} se envolveu em um deadlock e foi abortada por ser a transação mais recente!!!!!!"
                    esperando.append(i)
            elif (i[0].get_operation() == 'Read'):
                analise, t = bloqueios.check_locks(vetor_tran,i, 'RL', i[1])
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
                        novo_vetor_tran, transc = abortar_transaction(s, vetor_tran)
                        grafo.remove_node(transc)
                        return f"{transc} se envolveu em um deadlock e foi abortada por ser a transação mais recente!!!!!!"
                    esperando.append(i)
            elif (i[0].get_operation() == 'Update'):
                analise, t = bloqueios.check_locks(vetor_tran,i, 'WL', i[1])
                if analise != False:
                    bloqueios.lock_update(i)
                    s.append(i)
                else:
                    esperando.append(i)    
            elif(i[0].get_operation() == 'Commit'): 
                analise, t = verifica_leitura(vetor_tran, i[1])
                converte_certify(vetor_tran, i[1], k)            
                if analise == True:
                    esperando.append(i) 
                    grafo.add_edge(t,i[1].get_transaction())
                    if grafo_espera(grafo) == True:
                        novo_vetor_tran, transc = abortar_transaction(s, vetor_tran)
                        grafo.remove_node(transc)
                        return f"{transc} se envolveu em um deadlock e foi abortada por ser a transação mais recente!!!!!!"
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



