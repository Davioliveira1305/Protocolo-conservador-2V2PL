import objetos
import operations
import transactions
import protocolo
from bloqueios import lock_read
from bloqueios import lock_write
from bloqueios import liberar_locks
from bloqueios import lock_certify
from bloqueios import check_locks


# Inicializando o banco de dados
ob = objetos.Objetos('Banco', 'BD')

# Esquema com 1 Banco de dados, 2 areas, cada área com 2 tabelas, cada tabela com 2 páginas e cada página com 2 tuplas.
dic = objetos.criar_esquema(ob,2,2,2,2)


print('INSTALE A BIBLIOTECA NETWORKX PARA O GRAFO DE ESPERA!!!!')
scheduler = str(input("Digite o schedule: "))

# Cria a nossa matriz de operações a serem executadas
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
            if elementos[j + 3] == 'B':
                aux.append(elementos[j + 3])
                aux.append(elementos[j + 4])
            else:
                aux.append(elementos[j + 3])
                aux.append(elementos[j + 4])
                aux.append(elementos[j + 5])
                if elementos[j + 6] != ')': aux.append(elementos[j +6])
            vetor.append(dic[''.join(aux)])
            vetor_tran.append(vetor)
        elif elementos[j] == 'W':
            vetor = []
            aux_1 = []
            vetor.append(operations.Operation('W'))
            vetor.append(transactions.Transaction(elementos[j + 1]))
            if elementos[j + 3] == 'B':
                aux_1.append(elementos[j + 3])
                aux_1.append(elementos[j + 4])
            else:
                aux_1.append(elementos[j + 3])
                aux_1.append(elementos[j + 4])
                aux_1.append(elementos[j + 5])
                if elementos[j + 6] != ')': aux.append(elementos[j +6])
            vetor.append(dic[''.join(aux_1)])
            vetor_tran.append(vetor)
        elif elementos[j] == 'C':
            vetor = []
            vetor.append(operations.Operation('C')) 
            vetor.append(transactions.Transaction(elementos[j + 1]))
            vetor_tran.append(vetor)
        elif elementos[j] == 'U':
            vetor = []
            aux = []
            vetor.append(operations.Operation('U'))
            vetor.append(transactions.Transaction(elementos[j + 1]))
            if elementos[j + 3] == 'B':
                aux.append(elementos[j + 3])
                aux.append(elementos[j + 4])
            else:
                aux.append(elementos[j + 3])
                aux.append(elementos[j + 4])
                aux.append(elementos[j + 5])
                if elementos[j + 6] != ')': aux.append(elementos[j +6])
            vetor.append(dic[''.join(aux)])
            vetor_tran.append(vetor)
    return vetor_tran
vetor_tran = cria_objetos(scheduler)
scheduler_correct = protocolo.protocolo(vetor_tran)


def descodifica(scheduler_correct):
    vetor = []
    for i in scheduler_correct:
        if i[0].get_operation() == 'Read': 
            vetor.append('R')
            vetor.append(i[1].get_index())
            vetor.append('(')
            vetor.append(i[2].get_id())
            vetor.append(')')
        elif i[0].get_operation() == 'Write': 
            vetor.append('W')
            vetor.append(i[1].get_index())
            vetor.append('(')
            vetor.append(i[2].get_id())
            vetor.append(')')
        elif i[0].get_operation() == 'Commit': 
            vetor.append('C')
            vetor.append(i[1].get_index())
        string_resultante = ''.join(vetor)
    return string_resultante
if type(scheduler_correct) == str: print(scheduler_correct)
else:
    print(f"Schedule correto = {descodifica(scheduler_correct)}")