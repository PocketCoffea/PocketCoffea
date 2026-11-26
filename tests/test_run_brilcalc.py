from pocket_coffea.scripts.lumi.run_brilcalc import lumi_by_era, read_brilcalc_csv

CSV = """#Data tag : 25v1 , Norm tag: normtag_BRIL.json
#run:fill,time,nls,ncms,delivered(/pb),recorded(/pb)
100:1,01/01/25 00:00:00,10,10,1.1,1.0
150:1,01/01/25 01:00:00,10,10,2.2,2.0
300:2,01/02/25 00:00:00,10,10,4.4,4.0
#Summary:
#nfill,nrun,nls,ncms,totdelivered(/pb),totrecorded(/pb)
#2,3,30,30,7.7,7.0
"""


def test_lumi_by_era(tmp_path):
    f = tmp_path / "brilcalc.csv"
    f.write_text(CSV)
    lumi = read_brilcalc_csv(f)
    assert lumi == {100: 1.0, 150: 2.0, 300: 4.0}
    result, outside = lumi_by_era(lumi, {"A": (100, 120), "B": (130, 200)})
    assert result == {"A": 1.0, "B": 2.0, "tot": 3.0}
    assert outside == 4.0
