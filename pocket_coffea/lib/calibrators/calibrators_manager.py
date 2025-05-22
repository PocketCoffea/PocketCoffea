from .calibrator import Calibrator
from collections import defaultdict
from typing import List


class CalibratorsManager():
    """
    This class manages the calibration of collections for each event.
    The list of calibrators objects to apply is stored in the class.
    Each calibrator object is initialized in order with parameters and event metadata.

    The CalibratorManager exposes the set of shape variations created by the calibrator sequence.
    The variations are used to create the list of variations used to fill the histograms and columns.

    The CalibratorManager keeps in memory the original collection. 
    Moreover the calibrator knows which collections are calibrated by each calibrator.
    If a calibrator needs the original collection, it can be passed by the manager.
    This can be useful if a calibrator needs to use the original collection to calibrate another one after a previous
    calibrator has already modified the collection.
    """
    
    def __init__(self, calibrators_list: List[Calibrator], 
                 events,
                 params,
                 metadata=None,
                 ):
        self.calibrator_types = calibrators_list
        self.calibrator_sequence = []
        self.calibrated_collections = defaultdict(list)
        self.events = events
        self.metadata = metadata

        # Initialize all the calibrators
        for calibrator in calibrator_types:
            if calibrator.isMC_only and not events.isMC:
                # do not run the calibrator on data if it is MC only
                continue
            
            C = calibrator(params, metadata)
            c.initialize(events)
            self.calibrator_sequence.append(C)
            # storing the list of calibrator touching a collection in a dictionary
            for calibrated_collection in C.calibrated_collections:
                self.calibrated_collections[calibrated_collection].append(C)
            
        # Create the list of variations
        self.available_variations = []
        for calibrator in self.calibrator_sequence:
            if calibrator.has_variations:
                for variation in calibrator.variations:
                    if variation not in self.available_variations:
                        # check if the variation is already in the list
                        # if not, add it
                        self.available_variations.append(variation)

    def calibrate(self, events, variation):
        '''Call the calibrator object with the variation name in sequence. '''
        self.original_coll = {}
        for calibrator in self.calibrator_sequence:
            colls = calibrator.calibrate(events, variation)
            for col in colls:
                if col not in self.original_coll:
                    # Store the original column only once
                    self.original_coll[col] = events[col]

        

