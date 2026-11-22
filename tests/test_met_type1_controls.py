import awkward as ak
import numpy as np
import pytest
from omegaconf import OmegaConf
from .test_met_type1_inputs import make_events
from pocket_coffea.lib.calibrators.common.common import METCalibrator


def run(events,is_mc=True,orig=None,**opts):
    config=dict(apply_MC=True,apply_data=True,MET_collection='PuppiMET',RawMET_collection='RawPuppiMET',CorrT1METJet_collection='CorrT1METJet',Jet_collection='Jet')
    config.update(opts)
    params=OmegaConf.create(dict(jets_calibration={},met_calibration={'2024':config}))
    calc=METCalibrator(params,dict(year='2024',isMC=is_mc),do_variations=False)
    return calc,calc.calibrate(events,orig or {},'nominal')


def xy(out):
    pt=np.asarray(out['PuppiMET.pt']);phi=np.asarray(out['PuppiMET.phi'])
    return np.array([pt[0]*np.cos(phi[0]),pt[0]*np.sin(phi[0])])


@pytest.mark.parametrize('collection', ['Jet','CorrT1METJet'])
@pytest.mark.parametrize('eta,inside',[(5.1999,True),(5.2,False),(-5.2,False)])
def test_eta_strict_boundary_in_both_collections(collection,eta,inside):
    e=make_events(low_em=.1)
    e[collection,'eta']=ak.Array([[eta]])
    _,out=run(e,max_abs_eta=5.2)
    expected=[90.,-10.]
    if not inside:expected[0 if collection=='Jet' else 1]=100. if collection=='Jet' else 0.
    np.testing.assert_allclose(xy(out),expected,atol=1e-10)


def test_met_jer_toggle_leaves_analysis_jets_unchanged():
    e=make_events(low_em=.1)
    for coll in ['Jet','CorrT1METJet']:e[coll,'jer_factor']=ak.Array([[1.2]])
    before=ak.to_list(e.Jet)
    _,nom=run(e);_,off=run(e,propagate_jer=False)
    np.testing.assert_allclose(xy(nom),[90.,-10.],atol=1e-10)
    np.testing.assert_allclose(xy(off),[100.-20./3,-20./3],atol=1e-10)
    assert ak.to_list(e.Jet)==before


def test_met_jer_missing_bookkeeping_is_explicit():
    with pytest.raises(ValueError,match='jer_factor'):
        run(make_events(),propagate_jer=False)


def test_vector_component_ledger_includes_leptons():
    e=make_events(low_em=.1)
    e['Electron']=ak.zip(dict(pt=[[11.]],phi=[[np.pi]]))
    e['Muon']=ak.zip(dict(pt=[[23.]],phi=[[np.pi/2]]))
    calc,out=run(e,orig={'Electron.pt':ak.Array([[10.]]),'Muon.pt':ak.Array([[20.]])},store_components=True)
    summed=np.array([sum(float(out['PuppiMET.delta_'+kind+'_'+axis][0]) for kind in ['jet','lowpt','electron','muon']) for axis in ['px','py']])
    np.testing.assert_allclose(xy(out),[91.,-13.],atol=1e-10)
    np.testing.assert_allclose(np.array([100.,0.])-summed,xy(out),atol=1e-10)
    assert set(out)<=set(calc.calibrated_collections)


def test_empty_typed_collections_are_finite():
    e=make_events();e['Jet']=e.Jet[:,:0];e['CorrT1METJet']=e.CorrT1METJet[:,:0]
    _,out=run(e,max_abs_eta=5.2,store_components=True)
    np.testing.assert_allclose(xy(out),[100.,0.],atol=1e-10)


def test_veto_map_explicit_corrected_collection_and_threshold(monkeypatch):
    from pocket_coffea.lib import jets as jet_lib,cut_functions as cuts
    e=make_events(low_em=.1)
    e['JetMET']=ak.with_field(e.Jet,ak.Array([[14.]]),'pt')
    calls=[]
    def jetid(events,jet_type,params,year,jet_collection=None):
        calls.append(jet_collection)
        return ak.ones_like(events[jet_collection].pt)*6
    class Map:
        def evaluate(self,kind,eta,phi):return np.ones(len(eta))
    monkeypatch.setattr(jet_lib,'compute_jetId',jetid)
    monkeypatch.setattr(cuts,'load_correction_set',lambda _: {'map':Map()})
    params=OmegaConf.create({'jet_scale_factors':{'vetomaps':{'2024':{'file':'synthetic','name':'map'}}}})
    old=cuts.get_JetVetoMap().get_mask(e,params,year='2024',sample='test',isMC=True)
    new=cuts.get_JetVetoMap(jet_collection='JetMET').get_mask(e,params,year='2024',sample='test',isMC=True)
    assert ak.to_list(old)==[False] and ak.to_list(new)==[True]
    assert calls==['Jet','JetMET']


@pytest.mark.parametrize("variation,jec,jer",[("JER",2.,1.4),("JER_eta0to1p93",2.,1.4),("JES_Total",2.*28./24.,1.2)])
def test_jer_bookkeeping_follows_selected_jet_variation(variation,jec,jer):
    import copy
    from pocket_coffea.lib.calibrators.common.common import JetsCalibrator
    nominal=ak.zip({"pt":[[24.]],"mass":[[2.4]],"pt_raw":[[10.]],"rawFactor":[[1.-10./24.]],
                    "jec_factor":[[2.]],"jer_factor":[[1.2]],f"pt_{variation}_up":[[28.]],f"mass_{variation}_up":[[2.8]]})
    calc=object.__new__(JetsCalibrator)
    calc.year='2024';calc.jet_calib_param=OmegaConf.create({'collection':{'2024':{'AK4PFPuppi':'Jet'}}})
    calc.jets_calibrated={'Jet':nominal}
    out={'Jet':copy.copy(nominal)}
    calc.apply_variation(out,'AK4PFPuppi',variation,'up')
    np.testing.assert_allclose(ak.to_numpy(ak.flatten(out['Jet'].jer_factor)),[jer])
    np.testing.assert_allclose(ak.to_numpy(ak.flatten(out['Jet'].jec_factor)),[jec])
    np.testing.assert_allclose(ak.to_numpy(ak.flatten(nominal.jer_factor)),[1.2])
