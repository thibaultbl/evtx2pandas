"""Evtx parser
"""

import math
import json
import uuid

import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa
import numpy as np
import dask.dataframe as dd
import dask
from typing import Dict, Iterable, Any, Union, List

from evtx import PyEvtxParser


class EvtxParser:
    """[summary]
    """
    def evtx_to_dask2(self, evtx_path: str, **kwargs) -> dd:
        parser = PyEvtxParser(evtx_path)

        columns = [
            'Event.#attributes.xmlns', 'Event.EventData.Details', 'Event.EventData.EventType', 'Event.EventData.Image',
            'Event.EventData.ProcessGuid', 'Event.EventData.ProcessId', 'Event.EventData.RuleName',
            'Event.EventData.TargetObject', 'Event.EventData.UtcTime', 'Event.System.Channel', 'Event.System.Computer',
            'Event.System.Correlation', 'Event.System.EventID', 'Event.System.EventRecordID',
            'Event.System.Execution.#attributes.ProcessID', 'Event.System.Execution.#attributes.ThreadID',
            'Event.System.Keywords', 'Event.System.Level', 'Event.System.Opcode',
            'Event.System.Provider.#attributes.Guid', 'Event.System.Provider.#attributes.Name',
            'Event.System.Security.#attributes.UserID', 'Event.System.Task',
            'Event.System.TimeCreated.#attributes.SystemTime', 'Event.System.Version', 'event_record_id', 'timestamp',
            'Event.EventData.CommandLine', 'Event.EventData.Company', 'Event.EventData.CurrentDirectory',
            'Event.EventData.Description', 'Event.EventData.FileVersion', 'Event.EventData.Hashes',
            'Event.EventData.IntegrityLevel', 'Event.EventData.LogonGuid', 'Event.EventData.LogonId',
            'Event.EventData.ParentCommandLine', 'Event.EventData.ParentImage', 'Event.EventData.ParentProcessGuid',
            'Event.EventData.ParentProcessId', 'Event.EventData.Product', 'Event.EventData.TerminalSessionId',
            'Event.EventData.User', 'Event.EventData.ImageLoaded', 'Event.EventData.Signature',
            'Event.EventData.SignatureStatus', 'Event.EventData.Signed', 'Event.EventData.CallTrace',
            'Event.EventData.GrantedAccess', 'Event.EventData.SourceImage', 'Event.EventData.SourceProcessGUID',
            'Event.EventData.SourceProcessId', 'Event.EventData.SourceThreadId', 'Event.EventData.TargetImage',
            'Event.EventData.TargetProcessGUID', 'Event.EventData.TargetProcessId', 'Event.EventData.CreationUtcTime',
            'Event.EventData.TargetFilename'
        ]

        def parse_record(x):
            temp_df = pd.json_normalize(json.loads(x["data"]))
            temp_df.loc[:, 'event_record_id'] = x['event_record_id']
            temp_df.loc[:, 'timestamp'] = x['timestamp']

            missing_col = list(set(columns) - set(temp_df.columns))

            temp_df.loc[:, missing_col] = np.nan

            return temp_df

        records = parser.records_json()
        res = []
        for record in records:


        res = []
        dfs = [dask.delayed(parse_record)(x) for x in parser.records_json()]
        df = dd.from_delayed(dfs,
                             meta={
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
                                 'event_record_id': 'object',
                                 'Event.EventData.ParentProcessGuid': 'object',
                                 'Event.System.Correlation': 'object',
                                 'Event.EventData.TargetObject': 'object',
                                 'Event.EventData.EventType': 'float64',
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
                                 'timestamp': 'object',
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
                                 'Event.EventData.Details': 'float64',
                                 'Event.EventData.CreationUtcTime': 'float64',
                                 'Event.EventData.Hashes': 'object'
                             },
                             verify_meta=False)
        return df

    def evtx_to_dask(self, evtx_path: Union[str, List[str]], nrows: int = math.inf, **kwargs) -> dd:
        filepath = f"/tmp/{str(uuid.uuid4())}"
        print(filepath)  # TODO: to delete

        self.evtx_to_csv(evtx_path, filepath, nrows, iterable=True, sep="$")

        types = {
            'data.Event.EventData.CallTrace': 'object',
            'data.Event.EventData.CreationUtcTime': 'object',
            'data.Event.EventData.GrantedAccess': 'object',
            'data.Event.EventData.SourceImage': 'object',
            'data.Event.EventData.SourceProcessGUID': 'object',
            'data.Event.EventData.TargetFilename': 'object',
            'data.Event.EventData.TargetImage': 'object',
            'data.Event.EventData.TargetProcessGUID': 'object',
            'data.Event.System.Opcode': int,
            'data.Event.System.Version': 'object',
            'data.Event.EventData.ProcessId': float,
            "data.Event.System.Version": int
        }

        types.update({k + ' ': v
                      for k, v in types.items()
                      })  # Adding space before columns names because it appear at csv loading time

        dask_df = dd.read_csv(filepath, sep="$", dtype=types, **kwargs)
        dask_df.columns = [x.strip() for x in dask_df.columns]  # Some columns name contains space

        return dask_df

    def evtx_to_json(self, evtx_path: str, output_path: str, nrows: int = math.inf):
        mydict = self.evtx_to_dict(evtx_path, nrows)
        with open(output_path, 'w+') as fp:
            fp.write("[")
            json.dump(next(mydict), fp)

            for row in mydict:
                fp.write(", \n")
                json.dump(row, fp)

            fp.write("] \n")

    def _write_chunck(self, chuncks: List[pd.DataFrame], columns: List[str], temp_filepath: str, sep: str):
        temp_df = pd.concat(chuncks, axis=0)
        new_columns = list(set(temp_df.columns) - set(columns))
        old_columns_not_in_df = list(set(columns) - set(temp_df.columns))
        columns = columns + new_columns
        temp_df.loc[:, old_columns_not_in_df] = np.nan

        temp_df = temp_df.loc[:, columns]  # reorder columns

        temp_df.to_csv(temp_filepath, index=False, mode="a", header=None, sep=sep)
        return columns

    def evtx_to_csv(self,
                    evtx_path: str,
                    output_path: str,
                    nrows: int = math.inf,
                    iterable: bool = False,
                    sep: str = ";",
                    chunck_size: int = 50000):
        df = self.evtx_to_df(evtx_path, nrows, iterable=iterable)
        if iterable:
            temp_filepath = f"/tmp/{str(uuid.uuid4())}"

            row = next(df)
            row.to_csv(temp_filepath, index=False, mode="w", sep=sep, header=None)
            columns = list(row.columns)

            chuncks = []
            for i, row in enumerate(df):
                print(f"Treating row {i}")
                chuncks.append(row)

                if len(chuncks) >= chunck_size:
                    columns = self._write_chunck(chuncks, columns, temp_filepath, sep)
                    chuncks = []

            columns = self._write_chunck(chuncks, columns, temp_filepath, sep)

            with open(temp_filepath
                      ) as file:  # Need to rewrite the whole file to have the header with all columns in order
                with open(output_path, "w") as outputfile:
                    outputfile.write(f"{sep}".join(columns) + " \n")
                    for row in file:
                        outputfile.write(row)

        else:
            df.to_csv(output_path, index=False, sep=sep)

    def _df_chunck(self, mydict: Dict[Any, Any]) -> Iterable[pd.DataFrame]:
        for row in mydict:
            yield self.dict_to_df(row)

    def evtx_to_df(self,
                   evtx_path: str,
                   nrows: int = math.inf,
                   iterable: bool = False) -> Union[pd.DataFrame, Iterable[pd.DataFrame]]:
        mydict = self.evtx_to_dict(evtx_path, nrows)

        if iterable:
            return self._df_chunck(mydict)
        else:
            return self.dict_to_df(mydict)

    def evtx_to_dict(self, evtx_path: str, nrows: int = math.inf) -> Iterable[Dict[Any, Any]]:
        parser = PyEvtxParser(evtx_path)

        for i, record in enumerate(parser.records_json()):
            record["data"] = json.loads(record["data"])  # Parsing "data" field as json

            yield record

            if i > nrows:
                break

    def dict_to_df(self, input: Dict) -> pd.DataFrame:
        return pd.json_normalize(input, max_level=None)
