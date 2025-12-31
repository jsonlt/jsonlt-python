"""Hypothesis strategies for JSONLT property-based testing."""

from hypothesis import strategies as st

from jsonlt._constants import MAX_INTEGER_KEY, MAX_TUPLE_ELEMENTS, MIN_INTEGER_KEY

# Key-related strategies (migrated from test_key_comparison.py)
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

# JSON primitive strategy
json_primitive_strategy = st.one_of(
    st.none(),
    st.booleans(),
    st.integers(),
    st.floats(allow_nan=False, allow_infinity=False),
    st.text(),
)

# JSON value strategy (recursive, bounded depth)
# Use st.recursive to generate nested structures
json_value_strategy = st.recursive(
    json_primitive_strategy,
    lambda children: st.one_of(
        st.lists(children, max_size=5),
        st.dictionaries(st.text(max_size=20), children, max_size=5),
    ),
    max_leaves=50,
)

# JSON object strategy (for records)
json_object_strategy = st.dictionaries(
    st.text(max_size=20).filter(
        lambda s: not s.startswith("$")
    ),  # No $-prefixed fields
    json_value_strategy,
    max_size=10,
)

# Field name strategy (no $-prefix for valid records)
field_name_strategy = st.text(min_size=1, max_size=20).filter(
    lambda s: not s.startswith("$")
)

# Key specifier strategy
scalar_key_specifier_strategy = field_name_strategy
tuple_key_specifier_strategy = (
    st.lists(field_name_strategy, min_size=2, max_size=5)
    .filter(lambda fields: len(fields) == len(set(fields)))
    .map(tuple)
)
key_specifier_strategy = st.one_of(
    scalar_key_specifier_strategy,
    tuple_key_specifier_strategy,
)
