import uproot
import correctionlib.schemav2 as cs

# Input ROOT file
root_file = "/work/gbonomel/ttHbb/dataMC_Run3/2024_dataset/pileup_corrlib/PU_weights_Summer24.histo.root"
output_file = "puWeights_2024.json"

# Step 1: List contents of the file
with uproot.open(root_file) as f:
    print("Contents of the file:")
    for key, obj in f.items():
        print(f"  {key} -> {type(obj)}")

'''
PU;1 -> <class 'uproot.models.TH.Model_TH1D_v3'>
PUup;1 -> <class 'uproot.models.TH.Model_TH1D_v3'>
PUdown;1 -> <class 'uproot.models.TH.Model_TH1D_v3'>
'''

with uproot.open(root_file) as f:
    h_nom = f["PU;1"]
    h_up = f["PUup;1"]
    h_down = f["PUdown;1"]

# Extract edges (same for all)
edges = h_nom.axes[0].edges()

# Build correction with categorical variations
corr = cs.Correction(
    name="Pileup",  # this should match params.pileupJSONfiles[year]['name']
    description=f"Pileup weights (nominal/up/down) from {root_file}",
    version=1,
    inputs=[
        cs.Variable(name="nTrueInt", type="real", description="True number of interactions"),
        cs.Variable(name="variation", type="string", description="nominal/up/down")
    ],
    output=cs.Variable(name="weight", type="real"),
    data=cs.Category(
        nodetype="category",
        input="variation",
        content=[
            cs.CategoryItem(
                key="nominal",
                value=cs.Binning(
                    nodetype="binning",
                    input="nTrueInt",
                    edges=edges.tolist(),
                    content=h_nom.values().tolist(),
                    flow="clamp"
                )
            ),
            cs.CategoryItem(
                key="up",
                value=cs.Binning(
                    nodetype="binning",
                    input="nTrueInt",
                    edges=edges.tolist(),
                    content=h_up.values().tolist(),
                    flow="clamp"
                )
            ),
            cs.CategoryItem(
                key="down",
                value=cs.Binning(
                    nodetype="binning",
                    input="nTrueInt",
                    edges=edges.tolist(),
                    content=h_down.values().tolist(),
                    flow="clamp"
                )
            )
        ]
    )
)

# Wrap into CorrectionSet
cset = cs.CorrectionSet(
    schema_version=2,
    corrections=[corr]
)

# Save JSON
with open(output_file, "w") as fout:
    fout.write(cset.model_dump_json(indent=2))

print(f"Saved correctionlib file to {output_file}")