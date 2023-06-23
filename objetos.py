
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
        self.version = 'Antiga'
    
    def converte_version(self, transaction):
        self.version = transaction

    def version_normal(self):
        self.version = 'Antiga'

    def get_id(self) -> str:
        return self.ID_Objeto  
    
    def get_tipo(self) -> str:
       return 'OB'
    
    def get_index(self) -> int:
        return self.index

    def __repr__(self) -> str:
        return f"ID_Objeto = {self.ID_Objeto} Versão = {self.version}"
    
    def __str__(self) -> str:
        return f"ID_Objeto = {self.ID_Objeto}, Versão = {self.version}"

"""
Função que define se um determinado objeto é parente do outro
"""
def parentes(predecessor: Objetos, sucessor: Objetos):
    objetos = ['Banco', 'Area', 'Tabela', 'Pagina', 'Tupla']    
    predecessor.parentes[sucessor.objeto].append(sucessor)
    sucessor.parentes[predecessor.objeto].append(predecessor)
    if predecessor.index - 1 < 0: return
    return parentes(predecessor.parentes[objetos[predecessor.index - 1]][0], sucessor)
        

"""
A função recebe a qtde de áreas(tablespace) de um banco de dados, a qtde de tabelas por áreas, a qtde
de páginas por tabela e a quantidade de tuplas por pagina. Associa esses obejtos a outros, obedecendo
as restrições e retorna um dicionário com os objetos pertence a esse esquema. 
"""
from typing import Dict
def criar_esquema(banco: Objetos, qnt_tuplas: int, qnt_paginas: int, qnt_tabelas: int, qnt_areas: int) -> Dict[str, Objetos]:
   
   dicionario = {banco.ID_Objeto : banco}
   
   for i in range(qnt_areas):
      area = Objetos('Area', f'AA{i + 1}')
      parentes(banco, area)
      dicionario[area.ID_Objeto] = area
   
   U = 1
   areas = banco.parentes['Area']
   for area in areas:
      for i in range(qnt_tabelas):
         tabela = Objetos('Tabela',f'TB{U + i}')
         parentes(area, tabela)
         dicionario[tabela.ID_Objeto] = tabela
      U = U + qnt_tabelas

   U = 1
   areas = banco.parentes['Area']
   for area in areas:
       tabelas = area.parentes['Tabela']
       for tabela in tabelas:
           for i in range(qnt_paginas):
               pagina = Objetos('Pagina',f'PG{U + i}')
               parentes(tabela, pagina)
               dicionario[pagina.ID_Objeto] = pagina
           U = U + qnt_paginas

   U = 1
   areas = banco.parentes['Area']
   for area in areas:
       tabelas = area.parentes['Tabela']
       for tabela in tabelas:
           paginas = tabela.parentes['Pagina']
           for pagina in paginas:
               for i in range(qnt_tuplas):
                   tupla = Objetos('Tupla',f'TP{U + i}')
                   parentes(pagina, tupla)
                   dicionario[tupla.ID_Objeto] = tupla
               U += qnt_tuplas               
   return dicionario