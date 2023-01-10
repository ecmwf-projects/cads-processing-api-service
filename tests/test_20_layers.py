from cads_processing_api_service import layers


def test_compute_combinations():
    assert layers.compute_combinations(dict()) == []

    result = layers.compute_combinations({"param1": {"1", "2"}})
    expected = [{"param1": "2"}, {"param1": "1"}]
    assert len(result) == len(expected) and all(
        combination in expected for combination in result
    )

    result = layers.compute_combinations({"param1": {"1", "2"}, "param2": {"a", "b"}})
    expected = [
        {"param1": "1", "param2": "b"},
        {"param1": "1", "param2": "a"},
        {"param1": "2", "param2": "b"},
        {"param1": "2", "param2": "a"},
    ]
    assert len(result) == len(expected) and all(
        combination in expected for combination in result
    )


def test_remove_duplicates():
    result = layers.remove_duplicates(
        [{"level": {"500"}, "param": {"Z", "T"}}, {"level": {"500"}, "param": {"Z"}}]
    )
    expected = [{"level": "500", "param": "Z"}, {"level": "500", "param": "T"}]
    assert len(result) == len(expected) and all(
        combination in expected for combination in result
    )


def test_estimate_layers():
    form = {
        "level": {"500", "850"},
        "param": {"Z", "T"},
    }

    constraints = [
        {"level": {"500"}, "param": {"Z", "T"}},
        {"level": {"850"}, "param": {"T"}},
    ]

    assert (
        layers.estimate_layers(
            form, {"param": {"Z", "T"}, "level": {"500"}}, constraints
        )
        == 2
    )
    assert (
        layers.estimate_layers(
            form, {"param": {"Z", "T"}, "level": {"500", "850"}}, constraints
        )
        == 3
    )

    form = {
        "level": {"500", "850"},
        "param": {"Z", "T"},
    }

    constraints = [
        {"level": {"500"}, "param": {"Z", "T"}},
        {"level": {"500"}, "param": {"Z"}},
        {"level": {"850"}, "param": {"T"}},
    ]

    assert (
        layers.estimate_layers(
            form, {"param": {"Z", "T"}, "level": {"500"}}, constraints, safe=False
        )
        == 3
    )
    assert (
        layers.estimate_layers(
            form, {"param": {"Z", "T"}, "level": {"500"}}, constraints, safe=True
        )
        == 2
    )

    form = {
        "time": {"12:00", "00:00"},
        "param": {"Z", "T"},
        "stat": {"daily_mean", "hourly"},
    }

    constraints = [
        {"param": {"Z"}, "time": {"12:00", "00:00"}, "stat": {"hourly"}},
        {"param": {"Z"}, "stat": {"daily_mean"}},
    ]

    assert (
        layers.estimate_layers(
            form, {"param": {"Z"}, "stat": {"daily_mean"}}, constraints
        )
        == 1
    )
    assert (
        layers.estimate_layers(
            form,
            {"param": {"Z"}, "time": {"12:00", "00:00"}, "stat": {"hourly"}},
            constraints,
        )
        == 2
    )
    assert (
        layers.estimate_layers(
            form,
            {"param": {"Z"}, "time": {"12:00", "00:00"}, "stat": {"daily_mean"}},
            constraints,
        )
        == 1
    )

    form = {
        "level": {"500", "850"},
        "param": {"Z", "T"},
        "number": {"1", "2", "3"},
        "model": {"a", "b"},
    }

    constraints = [
        {"level": {"500"}, "param": {"Z", "T"}},
        {"level": {"850"}, "param": {"T"}},
    ]

    assert (
        layers.estimate_layers(
            form,
            {
                "param": {"Z"},
                "level": {"500"},
                "number": {"1", "2"},
                "model": {"a", "b"},
            },
            constraints,
        )
        == 4
    )

    form = {
        "level": {"500", "850"},
        "param": {"Z", "T"},
    }

    constraints = [
        {"level": {"500"}, "param": {"Z", "T"}},
        {"level": {"850"}, "param": {"T"}},
        {"level": {"500"}, "param": {"T"}},
    ]

    selection = {"param": {"Z", "T"}, "level": {"500"}}

    assert layers.estimate_layers(form, selection, constraints, safe=True) == 2

    form = {
        "type": {"projection", "historical"},
        "ensemble": {"1", "2"},
        "time": {"00:00", "12:00"},
        "stat": {"hourly", "daily_mean"},
    }

    constraints = [
        {
            "type": {"projection"},
            "ensemble": {"1", "2"},
            "time": {"00:00", "12:00"},
            "stat": {"hourly"},
        },
        {"type": {"projection"}, "ensemble": {"1"}, "stat": {"daily_mean"}},
        {"type": {"historical"}, "time": {"00:00", "12:00"}, "stat": {"hourly"}},
        {"type": {"historical"}, "stat": {"daily_mean"}},
    ]

    selection = {
        "type": {"projection", "historical"},
        "ensemble": {"1"},
        "time": {"00:00"},
        "stat": {"hourly", "daily_mean"},
    }

    assert layers.estimate_layers(form, selection, constraints) == 4

    form = {
        "level": {"500", "850"},
        "time": {"12:00", "00:00"},
        "param": {"Z", "T"},
        "stat": {"daily_mean", "hourly"},
        "number": {"1", "2", "3"},
        "model": {"a", "b", "c"},
    }

    selection = {
        "param": {"Z", "T"},
        "level": {"500", "850"},
        "stat": {"daily_mean", "hourly"},
        "time": {"12:00", "00:00"},
        "number": {"1", "2"},
        "model": {"a", "b"},
    }

    constraints = [
        {
            "level": {"500"},
            "param": {"Z", "T"},
            "time": {"12:00", "00:00"},
            "stat": {"hourly"},
        },
        {"level": {"850"}, "param": {"T"}, "time": {"12:00"}, "stat": {"hourly"}},
        {"level": {"500"}, "param": {"Z", "T"}, "stat": {"daily_mean"}},
        {"level": {"850"}, "param": {"T"}, "stat": {"daily_mean"}},
    ]

    assert layers.estimate_layers(form, selection, constraints)
