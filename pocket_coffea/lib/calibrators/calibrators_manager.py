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

    The CalibratorManager keeps a dictionary of calibrated collections by each calibrator: the name is expected
    to have the format "collection.field" (e.g. "Electron.pt"). The CalibratorManager checkes that each calibrator
    returns the collections it is supposed to handle, otherwise it raises an error.

    The CalibratorManager keeps in memory the original collection. 
    Moreover the calibrator knows which collections are calibrated by each calibrator.
    If a calibrator needs the original collection, it can be passed by the manager.
    This can be useful if a calibrator needs to use the original collection to calibrate another one after a previous
    calibrator has already modified the collection.

    kwargs can be passed to the constructor to pass objects necessary for
    the calibrators to work, such as jme-factor, loaded once by the processor.  TO BE IMPROVED
    """
    
    def __init__(self, calibrators_list: List[Calibrator], 
                 events,
                 params,
                 metadata=None,
                 requested_calibrator_variations=None,
                 **kwargs
                 ):
        self.calibrator_list = calibrators_list
        self.calibrator_sequence = []
        self.calibrated_collections = defaultdict(list)
        self.metadata = metadata
        self.available_variations = ["nominal"]
        self.available_variations_bycalibrator = defaultdict(list)
        self.requested_calibrator_variations = requested_calibrator_variations
        self.original_coll = {}

        # Initialize all the calibrators
        for calibrator in self.calibrator_list:
            if calibrator.isMC_only and not metadata["isMC"]:
                # do not run the calibrator on data if it is MC only
                continue

            if requested_calibrator_variations is not None and calibrator.name not in requested_calibrator_variations:
                # If the calibrator is not in the list of requested variations, we initialize it without variations
                C = calibrator(params, metadata, do_variations=False, **kwargs)
            else:
                # If the calibrator is in the list of requested variations, we initialize it with variations
                C = calibrator(params, metadata, do_variations=True, **kwargs)
           
            C.initialize(events)
            self.calibrator_sequence.append(C)
            # storing the list of calibrator touching a collection in a dictionary
            for calibrated_collection in C.calibrated_collections:
                self.calibrated_collections[calibrated_collection].append(C)
            
        # Create the list of variations
        for calibrator in self.calibrator_sequence:
            if calibrator.has_variations:
                for variation in calibrator.variations:
                    if variation not in self.available_variations:
                        # check if the variation is already in the list
                        # if not, add it
                        self.available_variations.append(variation)
                        # Store the variations by calibrator        
                        self.available_variations_bycalibrator[calibrator.name].append(variation)


    def reset_events_to_original(self, events):
        '''Take the original collection and reset the events to that, practically undoing any calibration'''
        for col in self.original_coll:
            if "." not in col:
                # If the col is not in the format "collection.field", we store it as is
                events[col] = self.original_coll[col]
            else:
                # If the col is in the format "collection.field", we need to split it
                collection, field = col.split(".")
                events[collection, field] = self.original_coll[col]

        # Clear the original collection ict
        self.original_coll.clear()
    
                        
    def calibrate(self, events, variation, debug=False):
        '''Call the calibrator objects in sequence.
        The calibrators returns the collections to replace in the events.
        The original collections are stored in a dictionary and passed to the chain
        of calibrators in case they need it. 
        '''
        if variation not in self.available_variations:
            # This should never happens, as the configurator should 
            # filter the requested variations
            raise ValueError(f"Variation {variation} not available. Available variations: {self.available_variations}")
        
        applied_calibrators = []
        # Store the original collections before applying the calibrators
        for calibrator in self.calibrator_sequence:
            if debug:
                print(f"Applying calibrator: {calibrator.name} for variation: {variation}")
            # If the variation is not handled by the calibrator
            # it will return the nominal collection. 
            # we don't want to control this in the manager, we 
            # want to get back the collection to replace, also if it is the 
            # nominal one.
            colls = calibrator.calibrate(events, self.original_coll, variation, 
                                         already_applied_calibrators=applied_calibrators)
            if debug:
                print(f"Calibrator {calibrator.name} returned collections: {colls.keys()}")
            for col in colls:
                if col not in calibrator.calibrated_collections:
                    raise ValueError(f"Calibrator {calibrator.name} is trying to calibrated a collection that it does not declare to handle:{col}. ")
                if "." not in col:
                    if col not in self.original_coll:     
                        try:
                            # If the collection is not in the original collection, we store it
                            self.original_coll[col] = events[col]
                        except ValueError:
                            # This means that the column is not present in the events and it is created by the calibrator
                            # and it is not a problem
                            self.original_coll[col] = None
                    # replacing the value
                    events[col] = colls[col]
                else:
                    # If the col is in the format "collection.field", we need to split it
                    collection, field = col.split(".")
                    if col not in self.original_coll:
                        try:
                            self.original_coll[col] = events[collection, field]
                        except ValueError:
                            # This means that the column is not present in the events and it is created by the calibrator
                            # and it is not a problem
                            self.original_coll[col] = None
                    events[collection, field] = colls[col]
            # Keep track of the calibrators applied
            applied_calibrators.append(calibrator.name)
        return events


    def calibration_loop(self, events, variations=None, variations_for_calibrators=None, debug=False):
        '''Loop over all the requested variations and yield the
        modified events. Keep a reference to the original events.'''
        
        variations_toloop = []
        if variations is not None:
            # If variations are provided, we use them
            variations_toloop += variations
   
        if variations_for_calibrators is not None:
            # We add the variations for the calibrators to the list of variations
            for calibrator in variations_for_calibrators:
                variations_toloop += self.get_available_variations(calibrator)

        variations_toloop = list(set(variations_toloop))  # remove duplicates
        # The nominal is added by default
        variations_toloop = ["nominal"] + variations_toloop

        for variation in variations_toloop:
            if debug:
                print(f"Applying calibrators for variation: {variation}")
            # Call the calibrator objects in sequence
            # This will call all the calibrators in the sequence
            # for the given variation
            events_out = self.calibrate(events, variation, debug=debug)
            # Yield the modified events
            yield variation, events_out
            # Reset the events to the original collections
            self.reset_events_to_original(events)
            # This is needed to make sure that the next variation is handled properly 
            # in case calibrations are computed on the fly on the modified values
        

    def get_available_variations(self, calibrator_name=None):
        """
        '''Return the list of available variations for 
        a specific calibrator or all the variations
        if calibrator_name is None, return all the variations"""
        if calibrator_name is None:
            return self.available_variations
        else:
            return self.available_variations_bycalibrator.get(calibrator_name, [])