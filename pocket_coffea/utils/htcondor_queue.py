"""CERN queue durations and pure queue-selection helpers."""

QUEUES = [
    "espresso",
    "microcentury",
    "longlunch",
    "workday",
    "tomorrow",
    "testmatch",
    "nextweek",
]

QUEUE_SECONDS = {
    "espresso": 20 * 60,
    "microcentury": 60 * 60,
    "longlunch": 2 * 60 * 60,
    "workday": 8 * 60 * 60,
    "tomorrow": 24 * 60 * 60,
    "testmatch": 3 * 24 * 60 * 60,
    "nextweek": 7 * 24 * 60 * 60,
}


def queue_for_runtime(seconds, threshold_percent):
    limit = float(threshold_percent) / 100
    return next(
        (queue for queue in QUEUES if seconds < QUEUE_SECONDS[queue] * limit),
        QUEUES[-1],
    )


def next_queue(current, shift=1):
    """Advance a queue name, using the longest known queue for unknown names."""
    shift = max(0, int(shift))
    if shift == 0:
        return current
    if current not in QUEUES:
        return QUEUES[-1]
    return QUEUES[min(QUEUES.index(current) + shift, len(QUEUES) - 1)]
