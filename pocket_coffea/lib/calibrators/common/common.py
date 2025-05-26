from ..calibrator import Calibrator
import numpy as np
import awkward as ak

class JetsCalibrator(Calibrator):
    def __init__(self, params, metadata):
        super().__init__(params, metadata)
        # initialize variations
        

    def initialize(self, events):
        # initialize the calibrator
        pass

    def calibrate(self, events, orig_colls, variation):
        pass



class METCalibrator(Calibrator):
    def __init__(self, params, metadata):
        super().__init__(params, metadata)
        # initialize variations

    def initialize(self, events):
        # initialize the calibrator
        pass

    def calibrate(self, events, orig_colls, variation):
        pass


class ElectronsCalibrator(Calibrator):
    def __init__(self, params, metadata):
        super().__init__(params, metadata)
        # initialize variations

    def initialize(self, events):
        # initialize the calibrator
        pass

    def calibrate(self, events, variation):
        pass

class MuonsCalibrator(Calibrator):
    def __init__(self, params, metadata):
        super().__init__(params, metadata)
        # initialize variations

    def initialize(self, events):
        # initialize the calibrator
        pass

    def calibrate(self, events, variation):
        pass


default_calibrators_sequence = [
]