import objetos
import bloqueios
import operations
import transactions
import mainteste

# Inicializando o banco de dados
ob = objetos.Objetos('Banco', 'BD')

# Esquema com 1 Banco de dados, 2 areas, cada área com 2 tabelas, cada tabela com 2 páginas e cada página com 2 tuplas.
dic = objetos.criar_esquema(ob,2,2,2,2)

scheduler = 'W1(TB1)W3(TB1)'

vetor_tran = mainteste.cria_objetos(scheduler)



s = []
e = []
for k,i in enumerate(vetor_tran):
    if i[0].get_operation() == 'Write':
        if bloqueios.check_locks(i[2], 'WL', i[1]) != False:
            bloqueios.lock_write(i)
            i[2].converte_version(i[1])
            s.append(i)
        else:
            e.append(i)
            del vetor_tran[k]
    

            
print(s)
print(e)
print(vetor_tran)