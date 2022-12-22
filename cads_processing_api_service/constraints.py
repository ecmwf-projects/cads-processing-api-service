"""Main module of the request-constraints API."""
import copy
import urllib.parse
from typing import Any

import cads_catalogue.database
import requests  # type: ignore

from . import clients, config


def ensure_sequence(v: Any) -> list[Any] | tuple[Any]:
    if not isinstance(v, list | tuple):
        v = [v]
    return v  # type: ignore


def parse_constraints(
    constraints: list[dict[str, list[Any]]]
) -> list[dict[str, set[Any]]]:
    """
    Parse constraints for a given dataset. Convert dict[str, list[Any]] into dict[str, Set[Any]].

    :param constraints: constraints in JSON format
    :type: list[dict[str, list[Any]]]

    :rtype: list[dict[str, set[Any]]]:
    :return: list of dict[str, set[Any]] containing all constraints
    for a given dataset.

    """
    result = []
    for combination in constraints:
        parsed_combination = {}
        for field_name, field_values in combination.items():
            parsed_combination[field_name] = set(ensure_sequence(field_values))
        result.append(parsed_combination)
    return result


def parse_selection(selection: dict[str, list[Any] | str]) -> dict[str, set[Any]]:
    """
    Parse current selection and convert dict[str, list[Any]] into dict[str, set[Any]].

    :param selection: a dictionary containing the current selection
    :type: dict[str, list[Any]]

    :rtype: dict[str, set[Any]]:
    :return: a dict[str, set[Any]] containing the current selection.
    """
    result = {}
    for field_name, field_values in selection.items():
        result[field_name] = set(ensure_sequence(field_values))
    return result


def apply_constraints(
    form: dict[str, set[Any]],
    selection: dict[str, set[Any]],
    constraints: list[dict[str, set[Any]]],
) -> dict[str, list[Any]]:
    """
    Apply dataset constraints to the current selection.

    :param form: a dictionary of all selectable values
    grouped by field name
    :param constraints: a list of all constraints
    :param selection: a dictionary containing the current selection
    :return: a dictionary containing all values that should be left
    active for selection, in JSON format
    """
    always_valid = get_always_valid_params(form, constraints)

    form = copy.deepcopy(form)
    selection = copy.deepcopy(selection)
    for key, value in form.copy().items():
        if key not in get_keys(constraints):
            form.pop(key, None)
            selection.pop(key, None)

    result = get_form_state(form, selection, constraints)
    result.update(always_valid)

    return format_to_json(result)


def get_possible_values(
    form: dict[str, set[Any]],
    selection: dict[str, set[Any]],
    constraints: list[dict[str, set[Any]]],
) -> dict[str, set[Any]]:
    """
    Get possible values given the current selection.

    Works only for enumerated fields, i.e. fields with values
    that must be selected one by one (no ranges).
    Checks the current selection against all constraints.
    A combination is valid if every field contains
    at least one value from the current selection.
    If a combination is valid, its values are added to the pool
    of valid values (i.e. those that can still be selected without
    running into an invalid request).

    :param form: a dict of all selectable fields and values
    e.g. form = {
        "level": {"500", "850", "1000"},
        "param": {"Z", "T"},
        "step": {"24", "36", "48"},
        "number": {"1", "2", "3"}
    }
    :type: dict[str, set[Any]]:

    :param constraints: a list of dictionaries representing
    all constraints for a specific dataset
    e.g. constraints = [
        {"level": {"500"}, "param": {"Z", "T"}, "step": {"24", "36", "48"}},
        {"level": {"1000"}, "param": {"Z"}, "step": {"24", "48"}},
        {"level": {"850"}, "param": {"T"}, "step": {"36", "48"}},
    ]
    :type: list[dict[str, set[Any]]]:

    :param selection: a dictionary containing the current selection
    e.g. selection = {
        "param": {"T"},
        "level": {"850", "500"},
        "step": {"36"}
    }
    :type: dict[str, set[Any]]:

    :rtype: dict[str, set[Any]]
    :return: a dictionary containing all possible values given the current selection
    e.g.
    {'level': {'500', '850'}, 'param': {'T', 'Z'}, 'step': {'24', '36', '48'}}

    """
    result: dict[str, set[Any]] = {key: set() for key in form}
    for combination in constraints:
        ok = True
        for field_name, selected_values in selection.items():
            if field_name in combination.keys():
                if len(selected_values & combination[field_name]) == 0:
                    ok = False
                    break
            elif field_name in form.keys():
                ok = False
                break
            else:
                print(f'Error: invalid param "{field_name}"')
                raise KeyError
        if ok:
            for field_name, valid_values in combination.items():
                result[field_name] |= set(valid_values)

    return result


def format_to_json(result: dict[str, set[Any]]) -> dict[str, list[Any]]:
    """
    Convert dict[str, set[Any]] into dict[str, list[Any]].

    :param result: dict[str, set[Any]] containing a possible form state
    :type: dict[str, set[Any]]:

    :rtype: dict[str, list[Any]]
    :return: the same values in dict[str, list[Any]] format

    """
    return {k: sorted(v) for (k, v) in result.items()}


def get_form_state(
    form: dict[str, set[Any]],
    selection: dict[str, set[Any]],
    constraints: list[dict[str, set[Any]]],
) -> dict[str, set[Any]]:
    """
    Calls get_possible_values() once for each key in form.

    :param form: a dict of all selectable fields and values
    e.g. form = {
        "level": {"500", "850", "1000"},
        "param": {"Z", "T"},
        "step": {"24", "36", "48"},
        "number": {"1", "2", "3"}
    }
    :type: dict[str, set[Any]]:

    :param constraints: a list of dictionaries representing
    all constraints for a specific dataset
    e.g. constraints = [
        {"level": {"500"}, "param": {"Z", "T"}, "step": {"24", "36", "48"}},
        {"level": {"1000"}, "param": {"Z"}, "step": {"24", "48"}},
        {"level": {"850"}, "param": {"T"}, "step": {"36", "48"}},
    ]
    :type: list[dict[str, set[Any]]]:

    :param selection: a dictionary containing the current selection
    e.g. selection = {
        "param": {"T"},
        "level": {"850", "500"},
        "step": {"36"}
    }
    :type: dict[str, set[Any]]:

    :rtype: dict[str, set[Any]]
    :return: a dictionary containing all form values to be left active given the current selection

    e.g.
    {'level': {'500', '850'}, 'param': {'T', 'Z'}, 'step': {'24', '36', '48'}}

    """
    result: dict[str, set[Any]] = {key: set() for key in form}

    for key in form:
        sub_selection = selection.copy()
        if key in sub_selection:
            sub_selection.pop(key)
        sub_results = get_possible_values(form, sub_selection, constraints)
        result[key] = sub_results.setdefault(key, set())
    return result


def get_always_valid_params(
    form: dict[str, set[Any]],
    constraints: list[dict[str, set[Any]]],
) -> dict[str, set[Any]]:
    """
    Get always valid field and values.

    :param form: a dict of all selectable fields and values
    e.g. form = {
        "level": {"500", "850", "1000"},
        "param": {"Z", "T"},
        "step": {"24", "36", "48"},
        "number": {"1", "2", "3"}
    }
    :type: dict[str, set[Any]]:

    :param constraints: a list of dictionaries representing
    all constraints for a specific dataset
    e.g. constraints = [
        {"level": {"500"}, "param": {"Z", "T"}, "step": {"24", "36", "48"}},
        {"level": {"1000"}, "param": {"Z"}, "step": {"24", "48"}},
        {"level": {"850"}, "param": {"T"}, "step": {"36", "48"}},
    ]
    :type: list[dict[str, set[Any]]]:

    :rtype: dict[str, set[Any]]
    :return: A dictionary containing fields and values that are not constrained (i.e. they are always valid)

    """
    result: dict[str, set[Any]] = {}
    for field_name, field_values in form.items():
        if field_name not in get_keys(constraints):
            result.setdefault(field_name, field_values)
    return result


def parse_form(form: list[dict[str, Any]]) -> dict[str, set[Any]]:
    """
    Parse the form for a given dataset extracting the information on the possible selections.

    :param form: a dictionary containing
    all possible selections in JSON format
    :type: dict[str, list[Any]]

    :rtype: dict[str, set[Any]]:
    :return: a dict[str, set[Any]] containing all possible selections.
    """
    selections: dict[str, set[Any]] = {}
    for parameter in form:
        if parameter["type"] in ("StringListWidget", "StringChoiceWidget"):
            values = parameter["details"]["values"]
            values = ensure_sequence(values)
            selections[parameter["name"]] = set(values)
        elif parameter["type"] == "StringListArrayWidget":
            selections_p: set[str] = set([])
            for sub_parameter in parameter["details"]["groups"]:
                values = ensure_sequence(sub_parameter["values"])
                selections_p = selections_p | set(values)
            selections[parameter["name"]] = selections_p
        else:
            pass
    return selections


def validate_constraints(
    process_id: str, body: dict[str, dict[str, Any]]
) -> dict[str, list[str]]:

    settings = config.Settings()
    storage_url = settings.document_storage_url
    timeout = settings.document_storage_access_timeout

    session_obj = cads_catalogue.database.ensure_session_obj(None)
    record = cads_catalogue.database.Resource
    with session_obj() as session:
        dataset = clients.lookup_resource_by_id(process_id, record, session)

    form_url = urllib.parse.urljoin(storage_url, dataset.form)
    raw_form = requests.get(form_url, timeout=timeout).json()
    form = parse_form(raw_form)

    constraints_url = urllib.parse.urljoin(storage_url, dataset.constraints)
    raw_constraints = requests.get(constraints_url, timeout=timeout).json()
    constraints = parse_constraints(raw_constraints)

    selection = parse_selection(body["inputs"])

    return apply_constraints(form, selection, constraints)


def get_keys(constraints: list[dict[str, Any]]) -> set[str]:
    keys = set()
    for constraint in constraints:
        keys |= set(constraint.keys())
    return keys
