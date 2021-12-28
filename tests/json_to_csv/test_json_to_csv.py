import os
import itertools

import numpy as np
import pandas as pd

from evtx2pandas.json_to_csv import EvtxParser


def test_evtx_to_csv(tmpdir, expected_df):
    reader = EvtxParser()

    json_path = os.path.join(os.path.dirname(__file__), '../evtx_sample.evtx')

    temp_file = tmpdir.mkdir("sub").join("evtx.csv")

    reader.evtx_to_csv(json_path, output_path=temp_file)

    df = pd.read_csv(temp_file).head(2)

    expected_df.loc[:, "data.Event.EventData.RuleName"] = [np.nan, np.nan]
    pd.testing.assert_frame_equal(expected_df, df, check_dtype=False)


def test_evtx_to_df(expected_df):
    reader = EvtxParser()

    json_path = os.path.join(os.path.dirname(__file__), '../evtx_sample.evtx')

    df = reader.evtx_to_df(json_path)

    df = df.iloc[0:2].reset_index(drop=True)  # Checking only the first two rows

    pd.testing.assert_frame_equal(expected_df, df)

    # Check with chunk
    iterator_df = reader.evtx_to_df(json_path, iterable=True)

    df1 = next(iterator_df)
    df2 = next(iterator_df)
    df = pd.concat([df1, df2], axis=0).reset_index(drop=True)

    expected = expected_df.loc[:, df.columns].reset_index(drop=True)
    pd.testing.assert_frame_equal(expected, df, check_dtype=False)


def test_dict_to_df(example_dict):
    reader = EvtxParser()

    df = reader.dict_to_df(example_dict)

    expected = pd.DataFrame({
        "record_number": {
            "0": 1,
            "1": 2,
            "2": 3
        },
        "data.a": {
            "0": 1,
            "1": 2,
            "2": 3
        },
        "data.b.c": {
            "0": 4,
            "1": 8,
            "2": 8
        },
        "data.b.d.e": {
            "0": 8,
            "1": 8,
            "2": 8
        },
        "data.b.d.f": {
            "0": "test",
            "1": "test45",
            "2": "test45"
        },
        "data.b.g": {
            "0": "hello",
            "1": "hello",
            "2": "hello"
        }
    })

    expected = expected.reset_index(drop=True)

    pd.testing.assert_frame_equal(df, expected)
