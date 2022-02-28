from lib.objects import get_dilepton 

def selectDilepton(events):

	ll = get_dilepton(events.GoodElectron, events.GoodMuon)

	SFOS = ( ((events.nmuons == 2) & (events.nelectrons == 0)) | ((events.nmuons == 0) & (events.nelectrons == 2)) ) & (ll.charge == 0)
	not_SFOS = ( (events.nmuons == 1) & (events.nelectrons == 1) ) & (ll.charge == 0)

	mask_events_res   = ((events.nleps == 2) & (events.ngoodleps >= 1) & (ll.charge == 0) &
						(events.ngoodjets >= 2) & (events.btags_resolved > 1) & (events.MET.pt > 40) &
						(ll.mass > 20) & ((SFOS & ((ll.mass < 76) | (ll.mass > 106))) | not_SFOS) )