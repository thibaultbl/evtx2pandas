from pytest import fixture

@fixture
def example_dict():
    dict_example = [{'record_number': 1, 'data': {'a': 1, 'b': {'c': 4, 'd': {'e': 8, 'f': 'test'}, 'g': 'hello'}}}, {'record_number': 2, 'data': {'a': 2, 'b': {'c': 8, 'd': {'e': 8, 'f': 'test45'}, 'g': 'hello'}}}, {'record_number': 3, 'data': {'a': 3, 'b': {'c': 8, 'd': {'e': 8, 'f': 'test45'}, 'g': 'hello'}}}]

    return dict_example