from abc import ABC,abstractmethod

from gojjam.ingest.extractors.extractor_factory import ExtractorFactory
from gojjam.ingest.loaders.load_factory import LoadFactory

class BaseIngestEngine(ABC):

    def __init__(self,model):
        self.model = model
        self.extractor = ExtractorFactory.get_extractor(self.model)
        self.loader = LoadFactory.get_loader(self.model["sink_info"])
    
    @abstractmethod
    def run(self):
        pass