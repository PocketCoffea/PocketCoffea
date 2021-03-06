from PocketCoffea.lib.cut_definition import Cut
from PocketCoffea.lib.cut_functions import count_objects_gt

def NjetsNb(events,params, **kwargs):
    mask =  ((events.njet >= params["njet"] ) &
            (events.nbjet >= params["nbjet"]))
    return mask

def getNjetNb_cut(njet, nb):
    return Cut(
        name=f"{njet}jet-{nb}bjet",
        params ={"njet": njet, "nbjet": nb},
        function=NjetsNb
    )
