"""Type-1 inputs: independent Cartesian expectations, without external payloads."""
import awkward as ak
import numpy as np
import pytest
from omegaconf import OmegaConf
from pocket_coffea.lib.calibrators.common.common import METCalibrator


def make_events(low_em=0.95, low_pt=20., low_phi=np.pi/2, main_em=None):
    jets = dict(pt=[[20.]], rawFactor=[[0.5]], eta=[[0.]], phi=[[0.]],
                muonSubtrFactor=[[0.]], chEmEF=[[0.1]], neEmEF=[[0.1]])
    if main_em is not None:
        jets['EmEF'] = [[main_em]]
    low = dict(pt=[[low_pt]], rawPt=[[low_pt/2]], rawFactor=[[0.5]],
               eta=[[0.]], phi=[[low_phi]], muonSubtrFactor=[[0.]])
    if low_em is not None:
        low['EmEF'] = [[low_em]]
    return ak.Array(dict(Jet=ak.zip(jets), CorrT1METJet=ak.zip(low),
                         RawPuppiMET=dict(pt=[100.],phi=[0.]),
                         PuppiMET=dict(pt=[100.],phi=[0.])))


def calibrate(events):
    params=OmegaConf.create(dict(jets_calibration={}, met_calibration={'2024':dict(
        apply_MC=True,apply_data=True,MET_collection='PuppiMET',RawMET_collection='RawPuppiMET',
        CorrT1METJet_collection='CorrT1METJet',Jet_collection='Jet')}))
    out=METCalibrator(params,dict(year='2024',isMC=True),do_variations=False).calibrate(events,{},'nominal')
    return np.array(out['PuppiMET.pt'])*np.cos(out['PuppiMET.phi']), np.array(out['PuppiMET.pt'])*np.sin(out['PuppiMET.phi'])


@pytest.mark.parametrize('low_em,expected_y', [(0.95,0.),(0.9,0.),(0.89,-10.),(None,-10.)])
def test_lowpt_em_branch_controls_its_own_collection(low_em,expected_y):
    x,y=calibrate(make_events(low_em=low_em))
    np.testing.assert_allclose(x,[90.],atol=1e-10)
    np.testing.assert_allclose(y,[expected_y],atol=1e-10)


def test_main_em_field_does_not_require_lowpt_em_branch():
    x,y=calibrate(make_events(low_em=None,main_em=0.1))
    np.testing.assert_allclose([x[0],y[0]],[90.,-10.],atol=1e-10)


@pytest.mark.parametrize('low_pt,expected_y',[(15.,0.),(15.0001,-7.50005)])
def test_lowpt_strict_corrected_pt_threshold(low_pt,expected_y):
    x,y=calibrate(make_events(low_em=0.1,low_pt=low_pt))
    np.testing.assert_allclose([x[0],y[0]],[90.,expected_y],atol=1e-10)


def test_muon_direction_and_fraction_enter_both_vector_components():
    events=make_events(low_em=0.1,low_phi=0.)
    events['CorrT1METJet','muonSubtrFactor']=ak.Array([[0.2]])
    events['CorrT1METJet','muonSubtrDeltaPhi']=ak.Array([[np.pi/4]])
    x,y=calibrate(events)
    d=8./np.sqrt(2.)
    np.testing.assert_allclose([x[0],y[0]],[90.-d,-d],atol=1e-10)
