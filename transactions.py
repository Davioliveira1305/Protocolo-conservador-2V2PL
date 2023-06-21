"""
Classe que define um objeto associado a uma determinada transação
"""    
class Transaction:
  def __init__(self, t: str):
    self.transaction = f'T{t}'
    self.t = t
  
  def get_tipo(self) -> str:
     return 'T'
  
  def get_index(self) -> int:
     return self.t

  def get_transaction(self) -> str:
    return self.transaction
  
  def __repr__(self) -> str:
    return f'{self.transaction}'