import itertools
import math

from . import constraints


def compute_combinations(d):
    if not d:
        return []
    keys, values = zip(*d.items())
    return [dict(zip(keys, v)) for v in itertools.product(*values)]


def remove_duplicates(found):
    granules = []
    for d in found:
        granules += compute_combinations(d)
    granules = set([tuple(granule.items()) for granule in granules])
    return [dict(granule) for granule in granules]


def estimate_granules(form, selection, _constraints, safe=True):
    always_valid = constraints.get_always_valid_params(form, _constraints)
    selected_but_always_valid = {
        k: v for k, v in selection.items() if k in always_valid.keys()
    }
    always_valid_multiplier = math.prod(map(len, selected_but_always_valid.values()))
    selected_constrained = {
        k: v for k, v in selection.items() if k not in always_valid.keys()
    }
    found = []
    for constraint in _constraints:
        intersection = {}
        ok = True
        for key, values in constraint.items():
            if key in selected_constrained.keys():
                common = values.intersection(selected_constrained[key])
                if common:
                    intersection.update({key: common})
                else:
                    ok = False
                    break
            else:
                ok = False
                break
        if ok:
            if intersection not in found:
                found.append(intersection)
    if safe:
        found = remove_duplicates(found)
        return (len(found)) * max(1, always_valid_multiplier)
    else:
        return sum([math.prod([len(e) for e in d.values()]) for d in found]) * max(
            1, always_valid_multiplier
        )


def estimate_size(form, selection, _constraints, safe=True, granule_size=1):
    return estimate_granules(form, selection, _constraints, safe=safe) * granule_size
