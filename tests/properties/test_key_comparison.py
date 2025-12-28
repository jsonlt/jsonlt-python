from hypothesis import given, strategies as st

from jsonlt._constants import MAX_INTEGER_KEY, MAX_TUPLE_ELEMENTS, MIN_INTEGER_KEY
from jsonlt._keys import compare_keys

key_element_strategy = st.one_of(
    st.text(),
    st.integers(min_value=MIN_INTEGER_KEY, max_value=MAX_INTEGER_KEY),
)

key_strategy = st.one_of(
    st.text(),
    st.integers(min_value=MIN_INTEGER_KEY, max_value=MAX_INTEGER_KEY),
    st.tuples(*[key_element_strategy] * 1),
    st.tuples(*[key_element_strategy] * 2),
    st.lists(key_element_strategy, min_size=1, max_size=MAX_TUPLE_ELEMENTS).map(tuple),
)


class TestTotalOrderProperties:
    @given(key_strategy)
    def test_reflexivity(self, a: str | int | tuple[str | int, ...]) -> None:
        assert compare_keys(a, a) == 0

    @given(key_strategy, key_strategy)
    def test_antisymmetry(
        self, a: str | int | tuple[str | int, ...], b: str | int | tuple[str | int, ...]
    ) -> None:
        cmp_ab = compare_keys(a, b)
        cmp_ba = compare_keys(b, a)

        if cmp_ab <= 0 and cmp_ba <= 0:
            assert cmp_ab == 0
            assert cmp_ba == 0

    @given(key_strategy, key_strategy, key_strategy)
    def test_transitivity(
        self,
        a: str | int | tuple[str | int, ...],
        b: str | int | tuple[str | int, ...],
        c: str | int | tuple[str | int, ...],
    ) -> None:
        cmp_ab = compare_keys(a, b)
        cmp_bc = compare_keys(b, c)
        cmp_ac = compare_keys(a, c)

        if cmp_ab < 0 and cmp_bc < 0:
            assert cmp_ac < 0

    @given(key_strategy, key_strategy)
    def test_totality(
        self, a: str | int | tuple[str | int, ...], b: str | int | tuple[str | int, ...]
    ) -> None:
        cmp = compare_keys(a, b)
        assert cmp in {-1, 0, 1}


class TestComparisonConsistency:
    @given(key_strategy, key_strategy)
    def test_reverse_comparison(
        self, a: str | int | tuple[str | int, ...], b: str | int | tuple[str | int, ...]
    ) -> None:
        cmp_ab = compare_keys(a, b)
        cmp_ba = compare_keys(b, a)
        assert cmp_ab == -cmp_ba

    @given(key_strategy, key_strategy)
    def test_equality_symmetry(
        self, a: str | int | tuple[str | int, ...], b: str | int | tuple[str | int, ...]
    ) -> None:
        if compare_keys(a, b) == 0:
            assert compare_keys(b, a) == 0


class TestTypeOrdering:
    @given(
        st.integers(min_value=MIN_INTEGER_KEY, max_value=MAX_INTEGER_KEY),
        st.text(),
    )
    def test_integer_before_string(self, i: int, s: str) -> None:
        assert compare_keys(i, s) == -1
        assert compare_keys(s, i) == 1

    @given(
        st.text(),
        st.lists(key_element_strategy, min_size=1, max_size=MAX_TUPLE_ELEMENTS).map(
            tuple
        ),
    )
    def test_string_before_tuple(self, s: str, t: tuple[str | int, ...]) -> None:
        assert compare_keys(s, t) == -1
        assert compare_keys(t, s) == 1

    @given(
        st.integers(min_value=MIN_INTEGER_KEY, max_value=MAX_INTEGER_KEY),
        st.lists(key_element_strategy, min_size=1, max_size=MAX_TUPLE_ELEMENTS).map(
            tuple
        ),
    )
    def test_integer_before_tuple(self, i: int, t: tuple[str | int, ...]) -> None:
        assert compare_keys(i, t) == -1
        assert compare_keys(t, i) == 1
