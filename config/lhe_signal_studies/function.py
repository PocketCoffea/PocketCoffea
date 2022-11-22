from PocketCoffea.lib.cut_definition import Cut
import awkwark as ak

semilep_lhe = Cut(
    name="semilep_lhe",
    params = {},
    function= lambda events,params: (ak.sum( (abs(events.LHEPart.pdgId) >=11)&(abs(events.LHEPart.pdgId) <19), axis=1)==2)
)


dilep_lhe = Cut(
    name="dilep_lhe",
    params = {},
    function= lambda events,params: (ak.sum( (abs(events.LHEPart.pdgId) >=11)&(abs(events.LHEPart.pdgId) <19), axis=1)==4)
)

had_lhe = Cut(
    name="had_lhe",
    params = {},
    function= lambda events,params: (ak.sum( (abs(events.LHEPart.pdgId) >=11)&(abs(events.LHEPart.pdgId) <19), axis=1)==0)
)
