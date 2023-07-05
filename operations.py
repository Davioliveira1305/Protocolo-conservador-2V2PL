"""
Classe que define o tipo de operação realizado no scheduler
"""
class Operation:
  def __init__(self, tipo: str):
    if tipo == 'R': self.operation = 'Read'
    if tipo == 'W': self.operation = 'Write'
    if tipo == 'U': self.operation = 'Update'
    if tipo == 'C': self.operation = 'Commit'

  def get_operation(self) -> str:
    return self.operation
  
  def get_tipo(self) -> str:
     return 'OP'

  def __str__(self) -> str:
    return f'Operação = {self.operation}'

  def __repr__(self) -> str:
    return f'Operação = {self.operation}'