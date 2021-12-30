from pytest import fixture

import pandas as pd
import numpy as np


@fixture
def example_dict():
    dict_example = [{
        'record_number': 1,
        'data': {
            'a': 1,
            'b': {
                'c': 4,
                'd': {
                    'e': 8,
                    'f': 'test'
                },
                'g': 'hello'
            }
        }
    }, {
        'record_number': 2,
        'data': {
            'a': 2,
            'b': {
                'c': 8,
                'd': {
                    'e': 8,
                    'f': 'test45'
                },
                'g': 'hello'
            }
        }
    }, {
        'record_number': 3,
        'data': {
            'a': 3,
            'b': {
                'c': 8,
                'd': {
                    'e': 8,
                    'f': 'test45'
                },
                'g': 'hello'
            }
        }
    }]

    return dict_example


@fixture
def expected_df():
    expected = {
        'Event.EventData.CommandLine': {
            0: np.nan,
            1: np.nan
        },
        'Event.System.Keywords': {
            0: '0x8000000000000000',
            1: '0x8000000000000000'
        },
        'Event.System.TimeCreated.#attributes.SystemTime': {
            0: '2019-05-21T00:36:14.747769Z',
            1: '2019-05-21T00:36:14.747769Z'
        },
        'Event.System.Version': {
            0: 2,
            1: 2
        },
        'Event.EventData.LogonGuid': {
            0: np.nan,
            1: np.nan
        },
        'Event.EventData.Product': {
            0: np.nan,
            1: np.nan
        },
        'Event.#attributes.xmlns': {
            0: 'http://schemas.microsoft.com/win/2004/08/events/event',
            1: 'http://schemas.microsoft.com/win/2004/08/events/event'
        },
        'Event.EventData.ImageLoaded': {
            0: np.nan,
            1: np.nan
        },
        'Event.System.Security.#attributes.UserID': {
            0: 'S-1-5-18',
            1: 'S-1-5-18'
        },
        'Event.EventData.IntegrityLevel': {
            0: np.nan,
            1: np.nan
        },
        'Event.System.Provider.#attributes.Name': {
            0: 'Microsoft-Windows-Sysmon',
            1: 'Microsoft-Windows-Sysmon'
        },
        'Event.EventData.User': {
            0: np.nan,
            1: np.nan
        },
        'Event.EventData.Description': {
            0: np.nan,
            1: np.nan
        },
        'Event.System.Channel': {
            0: 'Microsoft-Windows-Sysmon/Operational',
            1: 'Microsoft-Windows-Sysmon/Operational'
        },
        'Event.EventData.CurrentDirectory': {
            0: np.nan,
            1: np.nan
        },
        'Event.EventData.TargetProcessId': {
            0: np.nan,
            1: np.nan
        },
        'Event.EventData.ParentProcessGuid': {
            0: np.nan,
            1: np.nan
        },
        'Event.System.Correlation': {
            0: None,
            1: None
        },
        'Event.EventData.TargetObject': {
            0:
            'HKLM\\SOFTWARE\\Microsoft\\Windows NT\\CurrentVersion\\NetworkList\\Profiles\\{6ACC3724-ED52-4080-9712-AD6B9F4CD7E3}\\CategoryType',
            1:
            'HKLM\\SOFTWARE\\Microsoft\\Windows NT\\CurrentVersion\\NetworkList\\Profiles\\{6ACC3724-ED52-4080-9712-AD6B9F4CD7E3}\\Category'
        },
        'Event.EventData.EventType': {
            0: 'SetValue',
            1: 'SetValue'
        },
        'Event.EventData.CallTrace': {
            0: np.nan,
            1: np.nan
        },
        'Event.EventData.GrantedAccess': {
            0: np.nan,
            1: np.nan
        },
        'Event.System.Task': {
            0: 13,
            1: 13
        },
        'Event.EventData.Image': {
            0: 'C:\\Windows\\system32\\svchost.exe',
            1: 'C:\\Windows\\system32\\svchost.exe'
        },
        'Event.EventData.RuleName': {
            0: '',
            1: ''
        },
        'Event.EventData.TerminalSessionId': {
            0: np.nan,
            1: np.nan
        },
        'Event.EventData.SourceThreadId': {
            0: np.nan,
            1: np.nan
        },
        'Event.System.EventID': {
            0: 13,
            1: 13
        },
        'Event.EventData.UtcTime': {
            0: '2019-05-21 00:36:14.737',
            1: '2019-05-21 00:36:14.737'
        },
        'Event.System.Computer': {
            0: 'IEWIN7',
            1: 'IEWIN7'
        },
        'Event.EventData.Signature': {
            0: np.nan,
            1: np.nan
        },
        'Event.EventData.SourceProcessId': {
            0: np.nan,
            1: np.nan
        },
        'Event.EventData.ProcessGuid': {
            0: '365ABB72-39CB-5CE3-0000-001046AA0000',
            1: '365ABB72-39CB-5CE3-0000-001046AA0000'
        },
        'Event.System.Execution.#attributes.ProcessID': {
            0: 3416,
            1: 3416
        },
        'Event.EventData.ParentCommandLine': {
            0: np.nan,
            1: np.nan
        },
        'Event.EventData.Signed': {
            0: np.nan,
            1: np.nan
        },
        'Event.EventData.LogonId': {
            0: np.nan,
            1: np.nan
        },
        'Event.EventData.ProcessId': {
            0: 816.0,
            1: 816.0
        },
        'Event.EventData.ParentImage': {
            0: np.nan,
            1: np.nan
        },
        'Event.EventData.SignatureStatus': {
            0: np.nan,
            1: np.nan
        },
        'Event.EventData.SourceImage': {
            0: np.nan,
            1: np.nan
        },
        'Event.EventData.FileVersion': {
            0: np.nan,
            1: np.nan
        },
        'Event.System.Opcode': {
            0: 0,
            1: 0
        },
        'Event.System.Execution.#attributes.ThreadID': {
            0: 3496,
            1: 3496
        },
        'Event.System.Level': {
            0: 4,
            1: 4
        },
        'Event.EventData.TargetImage': {
            0: np.nan,
            1: np.nan
        },
        'Event.EventData.TargetProcessGUID': {
            0: np.nan,
            1: np.nan
        },
        'Event.EventData.Company': {
            0: np.nan,
            1: np.nan
        },
        'Event.EventData.TargetFilename': {
            0: np.nan,
            1: np.nan
        },
        'Event.EventData.SourceProcessGUID': {
            0: np.nan,
            1: np.nan
        },
        'Event.EventData.ParentProcessId': {
            0: np.nan,
            1: np.nan
        },
        'Event.System.EventRecordID': {
            0: 388,
            1: 387
        },
        'Event.System.Provider.#attributes.Guid': {
            0: '5770385F-C22A-43E0-BF4C-06F5698FFBD9',
            1: '5770385F-C22A-43E0-BF4C-06F5698FFBD9'
        },
        'Event.EventData.Details': {
            0: 'DWORD (0x00000000)',
            1: 'DWORD (0x00000001)'
        },
        'Event.EventData.CreationUtcTime': {
            0: np.nan,
            1: np.nan
        },
        'Event.EventData.Hashes': {
            0: np.nan,
            1: np.nan
        },
        'event_record_id': {
            0: 19,
            1: 18
        }
    }

    expected = pd.DataFrame(expected).reset_index(drop=True)
    expected = expected.astype({
        'Event.EventData.CommandLine': 'object',
        'Event.System.Keywords': 'object',
        'Event.System.TimeCreated.#attributes.SystemTime': 'object',
        'Event.System.Version': 'object',
        'Event.EventData.LogonGuid': 'object',
        'Event.EventData.Product': 'object',
        'Event.#attributes.xmlns': 'object',
        'Event.EventData.ImageLoaded': 'object',
        'Event.System.Security.#attributes.UserID': 'object',
        'Event.EventData.IntegrityLevel': 'object',
        'Event.System.Provider.#attributes.Name': 'object',
        'Event.EventData.User': 'object',
        'Event.EventData.Description': 'object',
        'Event.System.Channel': 'object',
        'Event.EventData.CurrentDirectory': 'object',
        'Event.EventData.TargetProcessId': 'object',
        # 'event_record_id': 'object',
        'Event.EventData.ParentProcessGuid': 'object',
        'Event.System.Correlation': 'object',
        'Event.EventData.TargetObject': 'object',
        'Event.EventData.EventType': 'object',
        'Event.EventData.CallTrace': 'float64',
        'Event.EventData.GrantedAccess': 'float64',
        'Event.System.Task': 'object',
        'Event.EventData.Image': 'object',
        'Event.EventData.RuleName': 'object',
        'Event.EventData.TerminalSessionId': 'object',
        'Event.EventData.SourceThreadId': 'object',
        'Event.System.EventID': 'object',
        'Event.EventData.UtcTime': 'object',
        'Event.System.Computer': 'object',
        'Event.EventData.Signature': 'object',
        'Event.EventData.SourceProcessId': 'object',
        'Event.EventData.ProcessGuid': 'object',
        'Event.System.Execution.#attributes.ProcessID': 'object',
        'Event.EventData.ParentCommandLine': 'object',
        'Event.EventData.Signed': 'object',
        'Event.EventData.LogonId': 'object',
        'Event.EventData.ProcessId': 'object',
        'Event.EventData.ParentImage': 'object',
        'Event.EventData.SignatureStatus': 'object',
        'Event.EventData.SourceImage': 'object',
        # 'timestamp': 'object',
        'Event.EventData.FileVersion': 'object',
        'Event.System.Opcode': 'object',
        'Event.System.Execution.#attributes.ThreadID': 'object',
        'Event.System.Level': 'object',
        'Event.EventData.TargetImage': 'object',
        'Event.EventData.TargetProcessGUID': 'object',
        'Event.EventData.Company': 'object',
        'Event.EventData.TargetFilename': 'object',
        'Event.EventData.SourceProcessGUID': 'object',
        'Event.EventData.ParentProcessId': 'object',
        'Event.System.EventRecordID': 'object',
        'Event.System.Provider.#attributes.Guid': 'object',
        'Event.EventData.Details': 'object',
        'Event.EventData.CreationUtcTime': 'float64',
        'Event.EventData.Hashes': 'object'
    })

    return expected