from typing import Any, Dict, List, Set, Union

from cads_processing_api_service import constraints


form: List[Dict[str, Union[List[Any], str]]] = [
    {
        "details": {
            "groups": [{"values": ["Z"]}, {"values": ["T"]}],
        },
        "name": "param",
        "type": "StringListArrayWidget",
    },
    {
        "details": {"values": ["500", "850", "1000"]},
        "name": "level",
        "type": "StringListWidget",
    },
    {
        "details": {"values": ["24", "36", "48"]},
        "name": "step",
        "type": "StringListWidget",
    },
    {
        "details": {"values": ["1", "2", "3"]},
        "name": "number",
        "type": "StringChoiceWidget",
    },
]

parsed_form: Dict[str, Set[Any]] = {
    "level": {"500", "850", "1000"},
    "param": {"Z", "T"},
    "step": {"24", "36", "48"},
    "number": {"1", "2", "3"},
}

constraints: List[Dict[str, List[Any]]] = [
    {"level": ["500"], "param": ["Z", "T"], "step": ["24", "36", "48"]},
    {"level": ["1000"], "param": ["Z"], "step": ["24", "48"]},
    {"level": ["850"], "param": ["T"], "step": ["36", "48"]},
]

parsed_constraints: List[Dict[str, Set[Any]]] = [
    {"level": {"500"}, "param": {"Z", "T"}, "step": {"24", "36", "48"}},
    {"level": {"1000"}, "param": {"Z"}, "step": {"24", "48"}},
    {"level": {"850"}, "param": {"T"}, "step": {"36", "48"}},
]

selections: List[Dict[str, List[Any]]] = [
    {},  # 0
    {"number": ["1", "2"]},  # 1
    {"level": ["850"], "param": ["Z"]},  # 2
]

parsed_selections: List[Dict[str, Set[Any]]] = [
    {},  # 0
    {"number": {"1", "2"}},  # 1
    {"level": {"850"}, "param": {"Z"}},  # 2
]


def test_get_possible_values() -> None:
    form = {
        "level": {"500", "850"},
        "time": {"12:00", "00:00"},
        "param": {"Z", "T"},
        "stat": {"mean"},
    }

    constraints = [
        {"level": {"500"}, "param": {"Z", "T"}, "time": {"12:00", "00:00"}},
        {"level": {"850"}, "param": {"T"}, "time": {"12:00", "00:00"}},
        {"level": {"500"}, "param": {"Z", "T"}, "stat": {"mean"}},
    ]

    assert constraints.get_possible_values(form, {"stat": {"mean"}}, constraints) == {
        "level": {"500"},
        "time": set(),
        "param": {"Z", "T"},
        "stat": {"mean"},
    }
    assert constraints.get_possible_values(form, {"time": {"12:00"}}, constraints) == {
        "level": {"850", "500"},
        "time": {"12:00", "00:00"},
        "param": {"Z", "T"},
        "stat": set(),
    }
    assert constraints.get_possible_values(
        form, {"stat": {"mean"}, "time": {"12:00"}}, constraints
    ) == {"level": set(), "time": set(), "param": set(), "stat": set()}
    assert constraints.get_possible_values(form, {"param": {"Z"}}, constraints) == {
        "level": {"500"},
        "time": {"12:00", "00:00"},
        "param": {"Z", "T"},
        "stat": {"mean"},
    }
    assert constraints.get_possible_values(
        form, {"level": {"500", "850"}}, constraints
    ) == {
        "level": {"500", "850"},
        "time": {"12:00", "00:00"},
        "param": {"Z", "T"},
        "stat": {"mean"},
    }


def test_get_form_state() -> None:
    form = {
        "level": {"500", "850"},
        "param": {"Z", "T"},
    }

    constraints = [
        {"level": {"500"}, "param": {"Z"}},
        {"level": {"850"}, "param": {"T"}},
    ]

    assert constraints.get_form_state(form, {"level": {"500"}}, constraints) == {
        "level": {"500", "850"},
        "param": {"Z"},
    }


def test_apply_constraints() -> None:
    form = {"level": {"500", "850"}, "param": {"Z", "T"}, "number": {"1"}}

    constraints = [
        {"level": {"500"}, "param": {"Z"}},
        {"level": {"850"}, "param": {"T"}},
    ]

    assert constraints.apply_constraints(form, {"level": {"500"}}, constraints)[
        "number"
    ] == ["1"]


def test_parse_constraints() -> None:
    assert parsed_constraints == constraints.parse_constraints(constraints)
    assert [{}] == constraints.parse_constraints([{}])


def test_parse_form() -> None:
    assert parsed_form == constraints.parse_form(form)
    assert {} == constraints.parse_form([])


def test_parse_selection() -> None:
    for i in range(len(selections)):
        try:
            assert parsed_selections[i] == constraints.parse_selection(selections[i])
        except AssertionError:
            print(
                f"Iteration number {i} of " f"{test_parse_selection.__name__}() failed!"
            )
            raise AssertionError


def test_ensure_list() -> None:
    assert constraints.ensure_list([]) == []
    assert constraints.ensure_list(("1",)) == ("1",)
    assert constraints.ensure_list("1") == ["1"]
