import objetos
import operations
import transactions
import re
from bloqueios import lock_read
from bloqueios import lock_write
from bloqueios import liberar_locks
from bloqueios import lock_certify
from bloqueios import check_locks
import protocolo


scheduler = 'R1(TB1)R3(TP1)C4R2(TB4)W2(AA1)R3(TP1)'

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
            vetor.append(f'R{vetor[1].get_index()}({vetor[2].get_id()})')
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
            vetor.append(f'W{vetor[1].get_index()}({vetor[2].get_id()})')
            vetor_tran.append(vetor)
        if elementos[j] == 'C':
            vetor = []
            vetor.append(operations.Operation('C')) 
            vetor.append(transactions.Transaction(elementos[j + 1]))
            vetor.append(f'C{vetor[1].get_index()}')
            vetor_tran.append(vetor)
    return vetor_tran

def main():
    scheduler = str(input("Digite o schedule: "))
    # Inicializando o banco de dados
    ob = objetos.Objetos('Banco', 'BD')

    # Esquema com 1 Banco de dados, 2 areas, cada área com 2 tabelas, cada tabela com 2 páginas e cada página com 2 tuplas.
    dic = objetos.criar_esquema(ob,2,2,2,2)

    correct_scheduler = protocolo_2v(scheduler)
    print(correct_scheduler)

main()