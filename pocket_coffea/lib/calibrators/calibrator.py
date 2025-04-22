import numpy as np
import inspect
import awkward as ak
from abc import ABC, ABCMeta, abstractmethod
from typing import List, Dict, ClassVar

class CalibratorRegistry(ABCMeta):
    calibrator_classes = {}

    def __new__(metacls, name, bases, clsdict):
        cls = super().__new__(metacls, name, bases, clsdict)
        # Register the class name if it is a subclass of Calibrator
        for base in bases:
            if base.__name__ in ["Calibrator"]:
                # only save the subclasses of Calibrator
                if clsdict.get("name") is None:
                    # This happens when the class is reloaded by cloudpickle.
                    # If the name attribute is missing we skip the validation of the name
                    # and do not register the class.
                    # It is fine because the classes are checked at definition time, before
                    # cloudpickle is used.
                    return cls
                cal_name = clsdict["name"]
                if cal_name not in metacls.calibrator_classes:
                    metacls.calibrator_classes[cal_name] = cls
                else:
                    raise ValueError(f"Calibrator with name {cal_name} already registered. Please use a different name.")
        return cls
     

    @classmethod
    def get_calibrator(cls, name):
        return cls.calibrator_classes.get(name)

    
class Calibrator(ABC, metaclass=CalibratorRegistry):
    '''
    Abstract class for the calibrators.
    '''
    
    name: ClassVar[str] = "base_calibrator"
    has_variations: ClassVar[bool] = False
    isMC_only: ClassVar[bool] = False
    _variations: List[str] = [] # default empty variations
    calibrated_collections: List[str] = []
    
    def __init__(self, params=None, metadata=None):
        self._params = params
        self._metadata = metadata
        # Variations must be setup in the constructor of the derived class.
        
    @abstractmethod
    def initialize(self, events):
        ''' Method called once for chunk for the calibrator to prepare
        all the necessary data'''
        pass

    @property
    def variations(self):
        return self._variations

    @abstractmethod
    def calibrate(self, events, variation):
        ''' The events objects is passed to the calibrator:
        the calibrated_collections are replaced in the object depending
        on the variation key requested. The method is called also for variations
        not defined by the correct calibrator. In this was the calibrator can
        react and customize its output depending on the requested variation.
        '''
        pass


    def serialize(self, src_code=False):
        out = {
            "name": self.name,
            "class": {
                "name": self.__class__.__name__,
                "module": self.__class__.__module__,
                "src_file": inspect.getsourcefile(self.__class__),
                "f_hash": hash(self.__class__)
                
            }
        }
        if src_code:
            out["class"]["src_code"] = inspect.getsource(self.__class__)
        return out
