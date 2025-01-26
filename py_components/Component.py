from abc import ABC, abstractmethod

# Base Component Interface
class Component(ABC):
    @abstractmethod
    def execute(self, df):
        pass