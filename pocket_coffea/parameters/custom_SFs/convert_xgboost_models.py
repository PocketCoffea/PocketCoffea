"""Re-save XGBoost model files using the current XGBoost version.

The model JSON files were saved with XGBoost < 1.6 and produce a deprecation
warning when loaded. Running this script once converts them to the current
format and silences the warning permanently.

Usage:
    python convert_xgboost_models.py
"""
import warnings
from pathlib import Path
import xgboost as xgb

MODEL_FILES = [
    Path(__file__).parent / "electron_promptMVA" / "Electron-mvaTTH.2022EE.weights_mvaISO.json",
    Path(__file__).parent / "muon_promptMVA" / "Muon-mvaTTH.2022EE.weights.json",
]

for path in MODEL_FILES:
    booster = xgb.Booster()
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        booster.load_model(str(path))
    booster.save_model(str(path))
    print(f"Converted: {path}")

print("Done.")
