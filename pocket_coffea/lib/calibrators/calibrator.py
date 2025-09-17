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
    # The calibrated collections format is expected to be "collection.field"
    calibrated_collections: List[str] = []
    
    def __init__(self, params=None, metadata=None, do_variations=True, **kwargs):
        '''
        The constructor of the calibrator. It receives the params and metadata
        dictionaries. The derived class can use them to configure the calibrator.
        The do_variations flag is used to enable or disable the variations: that's used 
        by the calibrators manager to signal to the calibrator that the variations
        are available but not requested for this chunk (from configuration)'''
        self.params = params
        self.metadata = metadata
        self.isMC = self.metadata["isMC"]
        self.year = self.metadata["year"]
        self.do_variations = do_variations  
        # Variations must be setup in the constructor of the derived class.
        
    @abstractmethod
    def initialize(self, events):
        ''' Method called once for chunk for the calibrator to prepare
        all the necessary data.
        The list of the variations is set here depending on the chunk metadata.
        N.B: the events object is passed to the calibrator before any calibrator is applied.
        If the calibrator prepares the variation values here, it is not incorporating 
        the changes that may be applied by other calibrators. If the user want
        to prepare the variations based on the events calibrated in the chain, 
        it should be done in the calibrate method.'''
        pass

    @property
    def variations(self):
        return self._variations

    
    @abstractmethod
    def calibrate(self, events, events_original_collections, variation, already_applied_calibrators=None):
        ''' The events objects is passed to the calibrator alongside
        a dictionary with the original collection. 
        the calibrated_collections are computed and returned to the manager.
        The method is called also for variations
        not defined by the correct calibrator. In this was the calibrator can
        react and customize its output depending on the requested variation.

        The calibrator MUST NOT replace in place the events collection.
        The CalibratorsManager will take care of replacing the collection 
        and of possible coordination between calibrators.

        If the variation is not handled by the calibrator, it should return
        the nominal collection, or the collection that is supposed to be
        returned in case of no variation.

        A list of the calibrators already applied to the events
        is passed to the calibrator. This is useful for the calibrators that
        need to know which calibrators have been already applied to the events.
        '''
        pass

    @classmethod
    def serialize(cls, src_code=False):
        out = {
            "name": cls.name,
            "class": {
                "name": cls.__class__.__name__,
                "module": cls.__class__.__module__,
                "src_file": inspect.getsourcefile(cls.__class__),
                "f_hash": hash(cls.__class__)
                
            }
        }
        if src_code:
            out["class"]["src_code"] = inspect.getsource(cls.__class__)
        return out
