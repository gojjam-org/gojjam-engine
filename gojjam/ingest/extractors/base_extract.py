from abc import ABC, abstractmethod
import pandas as pd

class BaseExtractor(ABC):
    def __init__(self, model_info):
        self.model_info = model_info

    @abstractmethod
    def extract(self) -> pd.DataFrame:
        """Each extractor must implement this and return a Pandas DataFrame"""
        pass