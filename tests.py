
"""
Classe que define um tipo de objeto, que pode ser um Banco de Dados, uma área, uma tabela, uma página
ou uma tupla.
"""
class Objetos:
    def __init__(self, tipo, ID):
        objetos = ['Banco', 'Area', 'Tabela', 'Pagina', 'Tupla']
        if not tipo.isnumeric():
           tipo = objetos.index(tipo)
        self.ID_Objeto = ID
        self.objeto = objetos[tipo]
        self.index = tipo
        self.parentes = {'Banco' : [], 'Area': [], 'Tabela': [], 'Pagina': [], 'Tupla': []}
        self.bloqueios = []

    def __repr__(self) -> str:
        return f"Tipo = {self.objeto} ID_Objeto = {self.ID_Objeto}"
    
    def __str__(self) -> str:
        return f"Tipo = {self.objeto} ID_Objeto = {self.ID_Objeto}"


"""
Função que define se um determinado objeto é parente do outro
"""
def parentes(predecessor: Objetos, sucessor: Objetos):
    objetos = ['Banco', 'Area', 'Tabela', 'Pagina', 'Tupla']    
    predecessor.parentes[sucessor.objeto].append(sucessor)
    sucessor.parentes[predecessor.objeto].append(predecessor)
    if predecessor.index - 1 < 0: return
    return parentes(predecessor.parentes[objetos[predecessor.index - 1]][0], sucessor)
        

from typing import Dict
def gerenerateEsquema(banco, n_tuplas, n_paginas, n_tabelas, n_areas) -> Dict[str, Objetos]:

    dicionário = {banco.ID_Objeto:banco}

    for areai in range(n_areas):
        area = Objetos('Area', f'AA{areai + 1}')
        parentes(banco, area)
        dicionário[area.ID_Objeto] = area
        for tabelai in range(n_tabelas):
            tabela = Objetos('Tabela', f'TB{tabelai + 1}')
            parentes(area, tabela)
            dicionário[tabela.ID_Objeto] = tabela
            for paginai in range(n_paginas):
                pagina = Objetos('Pagina', f'PG{paginai + 1}')
                parentes(tabela, pagina)
                dicionário[pagina.ID_Objeto] = pagina
                for tuplai in range(n_tuplas):
                    tupla = Objetos('Tupla', f'TP{tuplai + 1}')
                    parentes(pagina, tupla)
                    dicionário[tupla.ID_Objeto] = tupla
    return dicionário


vetor = [1,2,3,4,5]
for i, k in enumerate(vetor):
    print(k)
    if i == 2: break