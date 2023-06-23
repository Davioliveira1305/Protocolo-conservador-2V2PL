import objetos
import bloqueios
import operations
import transactions
import mainteste
import networkx as nx
import matplotlib.pyplot as plt
import copy

# Inicializando o banco de dados
ob = objetos.Objetos('Banco', 'BD')

# Esquema com 1 Banco de dados, 2 areas, cada área com 2 tabelas, cada tabela com 2 páginas e cada página com 2 tuplas.
dic = objetos.criar_esquema(ob,2,2,2,2)

scheduler = 'R1(TB1)W2(TB1)C2C1'

vetor_tran = mainteste.cria_objetos(scheduler)

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


print(len(vetor_tran))

def verifica_escrita(transaction, objeto):
    for i in objeto.bloqueios:
        if transaction.get_transaction() == i[1] and i[0] == 'WL': return True

def verifica_leitura(vetor, transaction):
    for j in vetor:
        for i in j[2].bloqueios:
            if transaction.get_transaction() != i[1] and i[0] == 'RL':
                return True

def locks_commit(vetor, transaction):
    for i in vetor:
        bloqueios.liberar_locks(i[2], transaction)

def protocolo(vetor_tran):
    esperando = []
    s = []
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
                cria_aresta(grafo,t, i[1].get_transaction())
                esperando.append(i)
        elif (i[0].get_operation() == 'Read'):
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
                cria_aresta(grafo,t, i[1].get_transaction())
                esperando.append(i)
        else:
            if verifica_leitura(s, i[1]) == True:
                esperando.append(i)
            else:
                locks_commit(s, i[1])
                s.append(i)
    if len(esperando) > 0: return protocolo(esperando)
    else: return s

scheduler = protocolo(vetor_tran)
print(scheduler)
    
def abortar_transaction(vetor):
    pass       

