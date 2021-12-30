import os
import json

import numpy as np
import pandas as pd

from evtx2pandas.json_to_csv import EvtxParser


def test_evtx_to_dask(tmpdir, expected_df):
    reader = EvtxParser()

    evtx_path = os.path.join(os.path.dirname(__file__), '../evtx_sample.evtx')

    dask_dd = reader.evtx_to_dask(evtx_path)

    df = dask_dd.compute()

    df.columns = [x.strip() for x in df.columns]

    df = df.sort_values(by="event_record_id", ascending=False).reset_index(drop=True)
    expected_df = expected_df.sort_values(by="event_record_id", ascending=False).reset_index(drop=True)

    pd.testing.assert_frame_equal(expected_df, df.iloc[0:2, :], check_dtype=False, check_like=True)


def test_evtx_to_csv(tmpdir, expected_df):
    reader = EvtxParser()

    json_path = os.path.join(os.path.dirname(__file__), '../evtx_sample.evtx')

    temp_file = tmpdir.mkdir("sub").join("evtx")
    temp_file = "/tmp/evtx.csv"

    reader.evtx_to_csv(json_path, output_path=temp_file)

    df = pd.read_csv(temp_file, sep=",")

    df = df.sort_values(by="event_record_id", ascending=False).reset_index(drop=True)
    expected_df = expected_df.sort_values(by="event_record_id", ascending=False).reset_index(drop=True)

    expected_df.loc[:, "Event.EventData.RuleName"] = [np.nan, np.nan]

    df.columns = [x.replace("data.", "") for x in df.columns]
    df = df.drop("timestamp", axis=1)

    pd.testing.assert_frame_equal(expected_df, df.head(2), check_dtype=False, check_names=False, check_like=True)


def test_evtx_to_df(expected_df):
    reader = EvtxParser()

    evtx_path = os.path.join(os.path.dirname(__file__), '../evtx_sample.evtx')

    df = reader.evtx_to_df(evtx_path)

    df = df.drop("timestamp", axis=1)
    df.columns = [x.replace("data.", "") for x in df.columns]

    df = df.sort_values(by="event_record_id", ascending=False).reset_index(drop=True)
    expected_df = expected_df.sort_values(by="event_record_id", ascending=False).reset_index(drop=True)

    pd.testing.assert_frame_equal(expected_df, df.iloc[0:2], check_dtype=False, check_like=True)


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
