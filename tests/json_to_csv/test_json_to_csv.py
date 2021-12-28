import os

import numpy as np
import pandas as pd
import pathlib

from evtx2pandas.json_to_csv import evtxJsonParser


def test_evtx_to_df():
    reader = evtxJsonParser()

    json_path = os.path.join(os.path.dirname(__file__), '../evtx_sample.evtx')

    df = reader.evtx_to_df(json_path)

    print(df.iloc[0:2].to_json())

    expected = {
        "event_record_id": {
            "0": 19,
            "1": 18
        },
        "timestamp": {
            "0": "1601-01-01 00:00:00 UTC",
            "1": "2019-05-21 00:36:14.747769 UTC"
        },
        "data.Event.#attributes.xmlns": {
            "0": "http://schemas.microsoft.com/win/2004/08/events/event",
            "1": "http://schemas.microsoft.com/win/2004/08/events/event"
        },
        "data.Event.EventData.Details": {
            "0": "DWORD (0x00000000)",
            "1": "DWORD (0x00000001)"
        },
        "data.Event.EventData.EventType": {
            "0": "SetValue",
            "1": "SetValue"
        },
        "data.Event.EventData.Image": {
            "0": "C:\\Windows\\system32\\svchost.exe",
            "1": "C:\\Windows\\system32\\svchost.exe"
        },
        "data.Event.EventData.ProcessGuid": {
            "0": "365ABB72-39CB-5CE3-0000-001046AA0000",
            "1": "365ABB72-39CB-5CE3-0000-001046AA0000"
        },
        "data.Event.EventData.ProcessId": {
            "0": 816.0,
            "1": 816.0
        },
        "data.Event.EventData.RuleName": {
            "0": "",
            "1": ""
        },
        "data.Event.EventData.TargetObject": {
            "0":
            "HKLM\\SOFTWARE\\Microsoft\\Windows NT\\CurrentVersion\\NetworkList\\Profiles\\{6ACC3724-ED52-4080-9712-AD6B9F4CD7E3}\\CategoryType",
            "1":
            "HKLM\\SOFTWARE\\Microsoft\\Windows NT\\CurrentVersion\\NetworkList\\Profiles\\{6ACC3724-ED52-4080-9712-AD6B9F4CD7E3}\\Category"
        },
        "data.Event.EventData.UtcTime": {
            "0": "2019-05-21 00:36:14.737",
            "1": "2019-05-21 00:36:14.737"
        },
        "data.Event.System.Channel": {
            "0": "Microsoft-Windows-Sysmon/Operational",
            "1": "Microsoft-Windows-Sysmon/Operational"
        },
        "data.Event.System.Computer": {
            "0": "IEWIN7",
            "1": "IEWIN7"
        },
        "data.Event.System.Correlation": {
            "0": None,
            "1": None
        },
        "data.Event.System.EventID": {
            "0": 13,
            "1": 13
        },
        "data.Event.System.EventRecordID": {
            "0": 388,
            "1": 387
        },
        "data.Event.System.Execution.#attributes.ProcessID": {
            "0": 3416,
            "1": 3416
        },
        "data.Event.System.Execution.#attributes.ThreadID": {
            "0": 3496,
            "1": 3496
        },
        "data.Event.System.Keywords": {
            "0": "0x8000000000000000",
            "1": "0x8000000000000000"
        },
        "data.Event.System.Level": {
            "0": 4,
            "1": 4
        },
        "data.Event.System.Opcode": {
            "0": 0,
            "1": 0
        },
        "data.Event.System.Provider.#attributes.Guid": {
            "0": "5770385F-C22A-43E0-BF4C-06F5698FFBD9",
            "1": "5770385F-C22A-43E0-BF4C-06F5698FFBD9"
        },
        "data.Event.System.Provider.#attributes.Name": {
            "0": "Microsoft-Windows-Sysmon",
            "1": "Microsoft-Windows-Sysmon"
        },
        "data.Event.System.Security.#attributes.UserID": {
            "0": "S-1-5-18",
            "1": "S-1-5-18"
        },
        "data.Event.System.Task": {
            "0": 13,
            "1": 13
        },
        "data.Event.System.TimeCreated.#attributes.SystemTime": {
            "0": "2019-05-21T00:36:14.747769Z",
            "1": "2019-05-21T00:36:14.747769Z"
        },
        "data.Event.System.Version": {
            "0": 2,
            "1": 2
        },
        "data.Event.EventData.CommandLine": {
            "0": None,
            "1": None
        },
        "data.Event.EventData.Company": {
            "0": None,
            "1": None
        },
        "data.Event.EventData.CurrentDirectory": {
            "0": None,
            "1": None
        },
        "data.Event.EventData.Description": {
            "0": None,
            "1": None
        },
        "data.Event.EventData.FileVersion": {
            "0": None,
            "1": None
        },
        "data.Event.EventData.Hashes": {
            "0": None,
            "1": None
        },
        "data.Event.EventData.IntegrityLevel": {
            "0": None,
            "1": None
        },
        "data.Event.EventData.LogonGuid": {
            "0": None,
            "1": None
        },
        "data.Event.EventData.LogonId": {
            "0": None,
            "1": None
        },
        "data.Event.EventData.ParentCommandLine": {
            "0": None,
            "1": None
        },
        "data.Event.EventData.ParentImage": {
            "0": None,
            "1": None
        },
        "data.Event.EventData.ParentProcessGuid": {
            "0": None,
            "1": None
        },
        "data.Event.EventData.ParentProcessId": {
            "0": np.nan,
            "1": np.nan
        },
        "data.Event.EventData.Product": {
            "0": None,
            "1": None
        },
        "data.Event.EventData.TerminalSessionId": {
            "0": np.nan,
            "1": np.nan
        },
        "data.Event.EventData.User": {
            "0": None,
            "1": None
        },
        "data.Event.EventData.ImageLoaded": {
            "0": None,
            "1": None
        },
        "data.Event.EventData.Signature": {
            "0": None,
            "1": None
        },
        "data.Event.EventData.SignatureStatus": {
            "0": None,
            "1": None
        },
        "data.Event.EventData.Signed": {
            "0": None,
            "1": None
        },
        "data.Event.EventData.CallTrace": {
            "0": None,
            "1": None
        },
        "data.Event.EventData.GrantedAccess": {
            "0": None,
            "1": None
        },
        "data.Event.EventData.SourceImage": {
            "0": None,
            "1": None
        },
        "data.Event.EventData.SourceProcessGUID": {
            "0": None,
            "1": None
        },
        "data.Event.EventData.SourceProcessId": {
            "0": np.nan,
            "1": np.nan
        },
        "data.Event.EventData.SourceThreadId": {
            "0": np.nan,
            "1": np.nan
        },
        "data.Event.EventData.TargetImage": {
            "0": None,
            "1": None
        },
        "data.Event.EventData.TargetProcessGUID": {
            "0": None,
            "1": None
        },
        "data.Event.EventData.TargetProcessId": {
            "0": np.nan,
            "1": np.nan
        },
        "data.Event.EventData.CreationUtcTime": {
            "0": None,
            "1": None
        },
        "data.Event.EventData.TargetFilename": {
            "0": None,
            "1": None
        }
    }

    expected = pd.DataFrame(expected).reset_index(drop=True)

    df = df.iloc[0:2].reset_index(drop=True)  # Checking only the first two rows

    pd.testing.assert_frame_equal(expected, df)


def test_dict_to_df(example_dict):
    reader = evtxJsonParser()

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
