"""Evtx parser
"""

import math
import json
import uuid
import os

import pandas as pd
import numpy as np
import dask.dataframe as dd
import dask
from typing import Dict, Iterable, Any, Union, List

from evtx import PyEvtxParser


class EvtxParser:
    """[summary]
    """

    dask_dtypes = {
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
    }

    def evtx_to_dask(self, evtx_path: str, **kwargs) -> dd:
        filepath = self.evtx_to_json(evtx_path)

        dask_df = dd.read_json(filepath, orient="record", encoding="utf-8")
        data = dask_df["data"].apply(lambda x: dict(x), meta=dict)

        def normalize(x, columns_order=list(self.dask_dtypes.keys())):
            df = pd.json_normalize(x)
            return df.loc[:, columns_order]

        data = data.map_partitions(normalize, meta=self.dask_dtypes)
        data['event_record_id'] = dask_df["Record"]
        return data

    def evtx_to_json(self, evtx_path: str, output_path: str = None, nrows: int = math.inf):
        if output_path is None:
            output_path = f"/tmp/{str(uuid.uuid4())}"

        cmd = f"./binary/evtx_dump -f {output_path} -o json {evtx_path}"
        so = os.popen(cmd).read()

        return output_path

    def _write_chunck(self, chuncks: List[pd.DataFrame], columns: List[str], temp_filepath: str, sep: str):
        temp_df = pd.concat(chuncks, axis=0)
        new_columns = list(set(temp_df.columns) - set(columns))
        old_columns_not_in_df = list(set(columns) - set(temp_df.columns))
        columns = columns + new_columns
        temp_df.loc[:, old_columns_not_in_df] = np.nan

        temp_df = temp_df.loc[:, columns]  # reorder columns

        temp_df.to_csv(temp_filepath, index=False, mode="a", header=None, sep=sep)
        return columns

    def evtx_to_csv(self, evtx_path: str, output_path: str, sep: str = ","):
        dask_dd = self.evtx_to_dask(evtx_path=evtx_path)
        dask_dd.to_csv(output_path, sep=sep, index=False)

    def _df_chunck(self, mydict: Dict[Any, Any]) -> Iterable[pd.DataFrame]:
        for row in mydict:
            yield self.dict_to_df(row)

    def evtx_to_df(self, evtx_path: str) -> Union[pd.DataFrame, Iterable[pd.DataFrame]]:
        return self.evtx_to_dask(evtx_path).compute()

    def evtx_to_dict(self, evtx_path: str, nrows: int = math.inf) -> Iterable[Dict[Any, Any]]:
        parser = PyEvtxParser(evtx_path)

        for i, record in enumerate(parser.records_json()):
            record["data"] = json.loads(record["data"])  # Parsing "data" field as json

            yield record

            if i > nrows:
                break

    def dict_to_df(self, input: Dict) -> pd.DataFrame:
        return pd.json_normalize(input, max_level=None)
