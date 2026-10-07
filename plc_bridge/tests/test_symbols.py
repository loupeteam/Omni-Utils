from plc_bridge import nest, nest_symbol


def test_br_colon_separator_nests_like_a_dot():
    data = nest({"Program:struct.member": 1, "Program:arr[1]": 2, "gVar": 3}, separators=":.")
    assert data == {"Program": {"struct": {"member": 1}, "arr": [None, 2]}, "gVar": 3}


def test_colon_is_part_of_the_name_for_a_dot_only_vendor():
    assert nest({"Program:var.member": 1}) == {"Program:var": {"member": 1}}


def test_nest_merges_symbols_of_one_struct():
    data = nest({"GVL.Axes[0].Pos": 1.5, "GVL.Axes[1].Pos": 2.5, "GVL.Axes[0].Vel": 9.0})
    assert data == {"GVL": {"Axes": [{"Pos": 1.5, "Vel": 9.0}, {"Pos": 2.5}]}}


def test_nest_returns_a_new_dict_each_time():
    flat = {"a.b": 1}
    first = nest(flat)
    first["a"]["b"] = 2
    assert nest(flat) == {"a": {"b": 1}}


def test_nest_symbol_writes_in_place_and_returns_target():
    target = {"keep": 1}
    assert nest_symbol(target, "a[0].b", 2) is target
    assert target == {"keep": 1, "a": [{"b": 2}]}


def test_array_of_arrays_member():
    assert nest({"m.rows[1].cols[2]": 7}) == {"m": {"rows": [None, {"cols": [None, None, 7]}]}}


import pytest


@pytest.mark.parametrize("symbol", ["a[0,1]", "a[1][2]", "a[]", "a[1", "a[x]", "a[9[", "[1[.b", "a.b[0,1].c"])
def test_unrepresentable_index_raises_naming_the_symbol(symbol):
    with pytest.raises(ValueError) as info:
        nest_symbol({}, symbol, 1)
    assert symbol in str(info.value)


def test_target_is_untouched_when_the_last_part_is_bad():
    target = {"keep": 1}
    with pytest.raises(ValueError):
        nest_symbol(target, "keep[0,1]", 2)
    assert target == {"keep": 1}


def test_empty_separators_are_rejected():
    with pytest.raises(ValueError):
        nest_symbol({}, "a.b", 1, separators="")
