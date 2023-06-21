import objetos
import operations
import networkx as nx
import matplotlib.pyplot as plt

scheduler = 'R1(x)C1W2(x)R2(y)W3(y)'
grau_granu = 'Pagina'

def cria_objetos(scheluder):
   elementos = list(scheluder)
   vetor_tran = []
   i = [i for i in range(len(elementos))]
   for j in i:
      if elementos[j] == 'R':
         vetor = []
         vetor.append(objetos.Operation('R'))
         vetor.append(objetos.Transaction(elementos[j + 1]))
         vetor.append(objetos.Objetos(grau_granu, elementos[j + 3]))
         vetor.append(f'R{vetor[1].get_index()}({vetor[2].get_id()})')
         vetor_tran.append(vetor)
      if elementos[j] == 'W':
         vetor = []
         vetor.append(objetos.Operation('W'))
         vetor.append(objetos.Transaction(elementos[j + 1]))
         vetor.append(objetos.Objetos(grau_granu, elementos[j + 3]))
         vetor.append(f'W{vetor[1].get_index()}({vetor[2].get_id()})')
         vetor_tran.append(vetor)
      if elementos[j] == 'C':
         vetor = []
         vetor.append(objetos.Operation('C')) 
         vetor.append(objetos.Transaction(elementos[j + 1]))
         vetor.append(f'C{vetor[1].get_index()}')
         vetor_tran.append(vetor)
   return vetor_tran

"""""
def compatible(historico, transaction, objeto, operacao):
   for i in range(len(historico)):
      if operacao.get_operation() == 'Read' and historico[i].get_bloqueio() == 'BW' and historico[i].get_transaction() != transaction and historico[i].get_objeto() == objeto:
         return False    
      if operacao.get_operation() == 'Read' and historico[i].get_bloqueio() == 'BR' and historico[i].get_transaction() != transaction and historico[i].get_objeto() == objeto:
         return True
      if operacao.get_operation() == 'Read' and historico[i].get_bloqueio() == 'BW' and historico[i].get_transaction() == transaction and historico[i].get_objeto() == objeto:
         return True
      if operacao.get_operation() == 'Write' and historico[i].get_bloqueio() == 'BR' and historico[i].get_transaction() != transaction and historico[i].get_objeto() == objeto:
         return False
      if operacao.get_operation() == 'Write' and historico[i].get_bloqueio() == 'BW' and historico[i].get_transaction() != transaction and historico[i].get_objeto() == objeto:
         return False
      if operacao.get_operation() == 'Write' and historico[i].get_bloqueio() == 'BW' and historico[i].get_transaction() == transaction and historico[i].get_objeto() == objeto:
         return True
      else:
        return True

def verifica_dl(vetor_tran):
   historico = []
   for i in range(len(vetor_tran)):
      if len(vetor_tran[i]) != 2:
         if vetor_tran[i][0].get_operation() == 'Read':
            if compatible(historico, vetor_tran[i][1].get_transaction(), vetor_tran[i][2].get_id(), vetor_tran[i][0]) == False:
               historico.append(objetosteste.Bloqueios(vetor_tran[i][2], 'R', vetor_tran[i][1],3))
            else:
               historico.append(objetosteste.Bloqueios(vetor_tran[i][2], 'R', vetor_tran[i][1],1))

         if vetor_tran[i][0].get_operation() == 'Write':
            if compatible(historico, vetor_tran[i][1].get_transaction(), vetor_tran[i][2].get_id(), vetor_tran[i][0]) == False:
               historico.append(objetosteste.Bloqueios(vetor_tran[i][2], 'W', vetor_tran[i][1],3))
            else:
               historico.append(objetosteste.Bloqueios(vetor_tran[i][2], 'W', vetor_tran[i][1],1))
   return historico
"""
      
vetor_tran = cria_objetos(scheduler)
    
# Criar um objeto do tipo grafo direcionado
grafo = nx.DiGraph()

# Adicionar nós
def cria_nos(grafo, vetor_tran):
   for i in range(len(vetor_tran)):
      for j in range(len(vetor_tran[i]) - 1):
         if vetor_tran[i][j].get_tipo() == 'T':
            grafo.add_node(f"{vetor_tran[i][j].get_transaction()}")

#Adiciona arestas
def cria_arestas(grafo, vetor_tran):
   for i in range(len(vetor_tran)):
      for j in range(i + 1, len(vetor_tran)): 
         if vetor_tran[i][0].get_operation() == 'Commit' or vetor_tran[j][0].get_operation() == 'Commit':
            break
            continue      
         if vetor_tran[i][0].get_operation() == 'Read' and vetor_tran[j][0].get_operation() == 'Read':
            continue
         if vetor_tran[i][1].get_transaction() == vetor_tran[j][1].get_transaction():
            continue
         if vetor_tran[i][2].get_id() != vetor_tran[j][2].get_id():
            continue        
         grafo.add_edge(vetor_tran[i][1].get_transaction(), vetor_tran[j][1].get_transaction())

cria_nos(grafo, vetor_tran)
cria_arestas(grafo, vetor_tran)

# Desenhar o grafo
nx.draw(grafo, with_labels=True)
plt.show()

# Verificar se o grafo possui ciclos
tem_ciclo = nx.is_directed_acyclic_graph(grafo)

if tem_ciclo:
    print("O grafo não possui ciclos.")
else:
    print("O grafo possui ciclos.")



         



