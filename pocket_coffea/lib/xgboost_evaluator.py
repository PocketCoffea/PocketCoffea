import numpy as np
import awkward as ak
import xgboost as xgb


class XGBoostEvaluator:
    """Base class for XGBoost BDT inference on lepton collections.

    Subclasses must define `feature_names` and implement `prepare_inputs`.
    """

    feature_names: list = []

    def __init__(self, model_path: str):
        self.booster = xgb.Booster()
        self.booster.load_model(model_path)
        self.booster.set_param("nthread", 1)  # Ensure single-threaded inference 

    def prepare_inputs(self, leptons, jets):
        """Return (inputs_2d, counts).

        inputs_2d : float32 numpy array of shape (n_flat_leptons, n_features)
        counts    : awkward or numpy array of per-event lepton counts for unflattening
        Jet-index-based lookups must be done here, before flattening leptons.
        """
        raise NotImplementedError

    def predict(self, inputs: np.ndarray) -> np.ndarray:
        """Run XGBoost inference and map raw log-odds to TMVA [-1, 1] scores."""
        dmat = xgb.DMatrix(inputs, feature_names=self.feature_names)
        raw = self.booster.predict(dmat)
        return 1.0 - 2.0 / (1.0 + np.exp(2.0 * raw))

    def evaluate(self, leptons, jets, mask=None) -> ak.Array:
        """Full pipeline: prepare → predict → unflatten back to event structure.

        mask : optional jagged bool array with the same shape as `leptons`.
               Where False the output score is set to -1 without running inference.
        """
        counts_orig = ak.num(leptons)
        n_total = int(ak.sum(counts_orig))

        if mask is not None:
            flat_mask = ak.to_numpy(ak.flatten(mask))
            eval_leptons = leptons[mask]
        else:
            flat_mask = None
            eval_leptons = leptons

        inputs, counts = self.prepare_inputs(eval_leptons, jets)
        n_flat = int(ak.sum(counts))

        if mask is not None:
            all_scores = np.full(n_total, -1.0, dtype=np.float32)
            if n_flat > 0:
                all_scores[flat_mask] = self.predict(inputs).astype(np.float32)
            return ak.unflatten(ak.from_numpy(all_scores), counts_orig)

        if n_flat == 0:
            return ak.unflatten(ak.from_numpy(np.array([], dtype=np.float32)), counts_orig)
        scores = self.predict(inputs)
        return ak.unflatten(ak.from_numpy(scores.astype(np.float32)), counts_orig)


class MuonMVAEvaluator(XGBoostEvaluator):
    """XGBoost evaluator for the muon mvaTTH BDT.

    Feature order matches VarIndex in Muon-mvaTTH.2022EE.weights.xml.
    """

    feature_names = [
        "Muon_pt",
        "Muon_eta",
        "Muon_pfRelIso03_all",
        "Muon_miniPFRelIso_chg",
        "Muon_miniRelIsoNeutral",
        "Muon_jetNDauCharged",
        "Muon_jetPtRelv2",
        "Muon_jetBTagDeepFlavB",
        "Muon_jetPtRatio",
        "Muon_sip3d",
        "Muon_log_dxy",
        "Muon_log_dz",
        "Muon_segmentComp",
    ]

    def prepare_inputs(self, leptons, jets):
        # Closest-jet b-tag score: jagged lookup must happen before flattening
        valid_jetIdx = ak.mask(leptons["jetIdx"], leptons["jetIdx"] != -1)
        btag = ak.flatten(
            ak.where(
                leptons["jetIdx"] == -1,
                0.0,
                ak.fill_none(jets[valid_jetIdx]["btagDeepFlavB"], -10.0),
            )
        )

        counts = ak.num(leptons)
        flat = ak.flatten(leptons)

        ptRatio = np.clip(
            1.0 / (1.0 + ak.to_numpy(flat["jetRelIso"])), None, 1.5
        )

        inputs = np.column_stack(
            [
                ak.to_numpy(flat["pt"]),
                ak.to_numpy(flat["eta"]),
                ak.to_numpy(flat["pfRelIso03_all"]),
                ak.to_numpy(flat["miniPFRelIso_chg"]),
                ak.to_numpy(flat["miniPFRelIso_all"] - flat["miniPFRelIso_chg"]),
                ak.to_numpy(flat["jetNDauCharged"]),
                ak.to_numpy(flat["jetPtRelv2"]),
                ak.to_numpy(btag).astype(np.float32),
                ptRatio,
                ak.to_numpy(flat["sip3d"]),
                np.log(np.abs(ak.to_numpy(flat["dxy"]))),
                np.log(np.abs(ak.to_numpy(flat["dz"]))),
                ak.to_numpy(flat["segmentComp"]),
            ]
        ).astype(np.float32)

        return inputs, counts


class ElectronMVAEvaluator(XGBoostEvaluator):
    """XGBoost evaluator for the electron mvaTTH BDT.

    Feature order matches VarIndex in Electron-mvaTTH.2022EE.weights.xml.
    Last feature is mvaIso instead of segmentComp.
    """

    feature_names = [
        "Electron_pt",
        "Electron_eta",
        "Electron_pfRelIso03_all",
        "Electron_miniPFRelIso_chg",
        "Electron_miniRelIsoNeutral",
        "Electron_jetNDauCharged",
        "Electron_jetPtRelv2",
        "Electron_jetBTagDeepFlavB",
        "Electron_jetPtRatio",
        "Electron_sip3d",
        "Electron_log_dxy",
        "Electron_log_dz",
        "Electron_mvaIso",
    ]

    def prepare_inputs(self, leptons, jets):
        # Closest-jet b-tag score: jagged lookup must happen before flattening
        valid_jetIdx = ak.mask(leptons["jetIdx"], leptons["jetIdx"] != -1)
        btag = ak.flatten(
            ak.where(
                leptons["jetIdx"] == -1,
                0.0,
                ak.fill_none(jets[valid_jetIdx]["btagDeepFlavB"], -10.0),
            )
        )

        counts = ak.num(leptons)
        flat = ak.flatten(leptons)

        ptRatio = np.clip(
            1.0 / (1.0 + ak.to_numpy(flat["jetRelIso"])), None, 1.5
        )

        inputs = np.column_stack(
            [
                ak.to_numpy(flat["pt"]),
                ak.to_numpy(flat["eta"]),
                ak.to_numpy(flat["pfRelIso03_all"]),
                ak.to_numpy(flat["miniPFRelIso_chg"]),
                ak.to_numpy(flat["miniPFRelIso_all"] - flat["miniPFRelIso_chg"]),
                ak.to_numpy(flat["jetNDauCharged"]),
                ak.to_numpy(flat["jetPtRelv2"]),
                ak.to_numpy(btag).astype(np.float32),
                ptRatio,
                ak.to_numpy(flat["sip3d"]),
                np.log(np.abs(ak.to_numpy(flat["dxy"]))),
                np.log(np.abs(ak.to_numpy(flat["dz"]))),
                ak.to_numpy(flat["mvaIso"]),
            ]
        ).astype(np.float32)

        return inputs, counts
