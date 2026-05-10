from abc import ABC,abstractmethod

class BaseCalculatedModel(ABC):

    @abstractmethod
    def calculate(self,query:str):
        pass