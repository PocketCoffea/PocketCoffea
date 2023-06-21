import time

def print_processing_stats(output, start_time, workers):
    '''
    '''
    stop_time = time.time()
    total_time = stop_time - start_time
    cutflow = output["cutflow"]
    tot_events_initial = sum([v for v in cutflow['initial'].values()])
    tot_events_skim = sum([v for v in cutflow['skim'].values()])
    tot_events_presel = sum([v for v in cutflow['presel'].values()])

    print(f"Number of events")
    print(f"\tTotal events: {tot_events_initial}")
    print(f"\tAfter skimming: {tot_events_skim}")
    print(f"\tAfter preselections: {tot_events_presel}")
    print(f"Total processing time: {total_time/60.:.2f} minutes")
    print(f"Number of workers: {workers}")
    print(f"Overall throughput:")
    print(f"\tTotal: {tot_events_initial/total_time:.2f} events/s")
    print(f"\tSkimmed events: {tot_events_skim/total_time:.2f} events/s")
    print(f"\tPreselected events: {tot_events_presel/total_time:.2f} events/s")
    print(f"Throughput by worker:")
    print(f"\tTotal: {tot_events_initial/total_time/workers:.2f} events/s/worker")
    print(f"\tSkimmed events: {tot_events_skim/total_time/workers:.2f} events/s/worker")
    print(f"\tPreselected events: {tot_events_presel/total_time/workers:.2f} events/s/worker")
    
    
