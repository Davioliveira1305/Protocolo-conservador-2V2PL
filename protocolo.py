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

scheduler = 'R2(TP1)R1(TP5)W2(TP6)R3(TP1)R1(TP6)W3(TP11)R2(TP11)W3(TP5)C3C1C2'

vetor_tran = main.cria_objetos(scheduler)

"""
bloqueios.lock_write(vetor_tran[0])
analise, t = bloqueios.check_locks(vetor_tran[0][2], 'WL', vetor_tran[0][1])
print(analise, t)
"""

# Criar um objeto do tipo grafo direcionado
grafo = nx.DiGraph()
# Adicionar nós
def cria_nos(grafo, vetor_tran):
   for i in range(len(vetor_tran)):
      for j in range(len(vetor_tran[i]) - 1):
         if vetor_tran[i][j].get_tipo() == 'T':
            grafo.add_node(f"{vetor_tran[i][j].get_transaction()}")

cria_nos(grafo, vetor_tran)

def cria_aresta(grafo, transaction1, transaction2):
    grafo.add_edge(transaction1, transaction2)

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
                    return True

def verifica_operation(vetor, transaction):
    for i in vetor:
        if i[1].get_transaction() == transaction.get_transaction(): return True

def locks_commit(vetor, transaction):
    for i in vetor:
        if i[0].operation != 'Commit':
            bloqueios.liberar_locks(i[2], transaction)


def protocolo(vetor_tran):
    s = []
    while True:        
        esperando = []
        for k,i in enumerate(vetor_tran):
            if i[0].get_operation() == 'Write':
                analise, t = bloqueios.check_locks(i[2], 'WL', i[1])           
                if analise != False:
                    bloqueios.lock_write(i)
                    i[2].converte_version(i[1]) 
                    objeto_copy = copy.deepcopy(i)
                    s.append(objeto_copy)
                    i[2].version_normal()
                else:
                    esperando.append(i)
            if (i[0].get_operation() == 'Read'):
                analise, t = bloqueios.check_locks(i[2], 'RL', i[1])
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
                    esperando.append(i)
            
            if(i[0].get_operation() == 'Commit'):
                if verifica_leitura(s, i[1]) == True:
                    esperando.append(i)                
                else:
                    locks_commit(vetor_tran, i[1])
                    locks_commit(s, i[1])
                    s.append(i)
        vetor_tran = esperando
        if len(vetor_tran) == 0: break
    return s
scheduler = protocolo(vetor_tran)

print(scheduler)

def abortar_transaction(vetor):
    pass       

