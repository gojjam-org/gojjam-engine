from abc import ABC,abstractmethod


class BaseLoader(ABC):
    def __init__(self,sink_config):
        self.sink_config = sink_config

    @abstractmethod
    def load(self):
        pass