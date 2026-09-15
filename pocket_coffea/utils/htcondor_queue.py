"""HTCondor ``+JobFlavour`` queue helpers shared by the manual-job tooling.

The CERN/lxplus condor pools schedule jobs by ``+JobFlavour`` — a named
wall-clock ladder from ``espresso`` (short) up to ``nextweek`` (long). When a
job is removed for hitting its time limit we bump it up the ladder; a user can
also force a specific flavour on resubmission.

These helpers used to be duplicated between
``pocket_coffea/scripts/check_jobs.py`` (``bump_jobqueue``) and
``pocket_coffea/executors/executors_lxplus.py`` (``update_queue`` / ``set_queue``).
They live here so there is a single implementation (and a single ``QUEUES``
ladder) that is independently unit-testable.

Pools whose ``.sub`` files use a different mechanism (e.g. the rubin/UMD pool
uses ``+MaxRuntime``, not ``+JobFlavour``) have no ``+JobFlavour`` line: both
helpers are then no-ops and report that nothing was changed.
"""

QUEUES = [
    "espresso",
    "microcentury",
    "longlunch",
    "workday",
    "tomorrow",
    "testmatch",
    "nextweek",
]


def _read_flavour(line):
    """Extract the flavour value from a ``+JobFlavour`` line, tolerating both
    ``+JobFlavour="x"`` and ``+JobFlavour = "x"`` spacing."""
    return line.split("=")[1].strip().replace('"', '')


def bump_queue(sub_file, shift=1):
    """Bump the ``+JobFlavour`` in ``sub_file`` up the ``QUEUES`` ladder by
    ``shift`` steps (capped at the longest queue).

    Returns the new flavour string, or ``None`` when the ``.sub`` has no
    ``+JobFlavour`` line (e.g. a ``+MaxRuntime``-based pool), in which case the
    file is left unchanged.
    """
    with open(sub_file) as f:
        lines = f.readlines()
    next_jf = None
    with open(sub_file, "w") as f:
        for line in lines:
            if "+JobFlavour" in line:
                jf = _read_flavour(line)
                if jf in QUEUES:
                    next_jf = QUEUES[min(QUEUES.index(jf) + shift, len(QUEUES) - 1)]
                else:
                    # Unknown flavour (e.g. a non-lxplus queue): bump to the longest
                    # known queue rather than raising ValueError on QUEUES.index.
                    next_jf = QUEUES[-1]
                f.write(f'+JobFlavour="{next_jf}"\n')
            else:
                f.write(line)
    # next_jf stays None if the .sub has no +JobFlavour line (e.g. rubin uses
    # +MaxRuntime); callers only log it, so return None instead of NameError-ing.
    return next_jf


def set_queue(sub_file, new_queue, job=None):
    """Force the ``+JobFlavour`` in ``sub_file`` to ``new_queue``.

    Unlike :func:`bump_queue` this writes an explicit value verbatim (used by
    ``check-jobs --recreate-queue``). Returns True when the flavour was actually
    changed, False when it was already ``new_queue`` or the ``.sub`` has no
    ``+JobFlavour`` line.
    """
    with open(sub_file) as f:
        lines = f.readlines()
    changed = False
    with open(sub_file, "w") as f:
        for line in lines:
            if "+JobFlavour" in line:
                old = _read_flavour(line)
                if old != new_queue:
                    label = f"{job}: " if job else ""
                    print(f"[queue] {label}{old} -> {new_queue}")
                    changed = True
                f.write(f'+JobFlavour="{new_queue}"\n')
            else:
                f.write(line)
    return changed
