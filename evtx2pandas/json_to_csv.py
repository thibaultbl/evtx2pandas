"""Evtx parser
"""

import math
import json
import uuid
import os
import itertools

import multiprocessing
from functools import partial
import pandas as pd
from pandas.core.indexes import multi
import numpy as np
import dask.dataframe as dd
import dask
from typing import Dict, Iterable, Any, Union, List

from evtx import PyEvtxParser

global columns
csv_lock = multiprocessing.Lock()
columns_lock = multiprocessing.Lock()

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

pd_dtypes = {"data." + k: v for k, v in dask_dtypes.items()}


def _write_to_csv(df: Iterable[pd.DataFrame], chunck_size: int, columns: List[str], temp_filepath: str,
                  sema: multiprocessing.Semaphore, sep: str):

    chuncks = list(itertools.islice(df, 0, chunck_size))
    write_chunck = partial(_write_chunck, columns, temp_filepath, sep, sema)
    processes = []
    while len(chuncks) > 0:
        p = multiprocessing.Process(target=write_chunck, args=(chuncks, ))
        p.start()
        processes.append(p)
        # _write_chunck(chuncks, columns, temp_filepath, sep, sema)
        chuncks = list(itertools.islice(df, 0, chunck_size))
    [p.join() for p in processes]


def _write_chunck(columns: List[str], temp_filepath: str, sep: str, sema: multiprocessing.Semaphore,
                  chuncks: List[pd.DataFrame]):
    sema.acquire(block=True)
    temp_df = pd.concat(chuncks, axis=0)

    columns_lock.acquire(block=True)
    new_columns = list(set(temp_df.columns) - set(columns))
    old_columns_not_in_df = list(set(columns) - set(temp_df.columns))
    columns.extend(new_columns)
    mycol = list(columns)
    columns_lock.release()

    temp_df.loc[:, old_columns_not_in_df] = np.nan

    temp_df = temp_df.loc[:, mycol]  # reorder columns

    temp_df = temp_df.astype({k: pd_dtypes.get(k, "object") for k in temp_df.columns}, errors="ignore")

    csv_lock.acquire(block=True)
    temp_df.to_csv(temp_filepath, index=False, mode="a", header=None, sep=sep)
    csv_lock.release()
    sema.release()


class EvtxParser:
    """[summary]
    """
    def _normalize(self, x, columns_order=list(dask_dtypes.keys())):
        df = pd.json_normalize(x)
        return df.loc[:, columns_order]

    def evtx_to_dask(self, evtx_path: str, **kwargs) -> dd:
        filepath = self.evtx_to_json(evtx_path)

        dask_df = dd.read_json(filepath, orient="record", encoding="utf-8")
        data = dask_df["data"].apply(dict, meta=dict)

        data = data.map_partitions(self._normalize, meta=dask_dtypes)
        data['event_record_id'] = dask_df["Record"]
        return data

    def evtx_to_json(self, evtx_path: str, output_path: str = None, nrows: int = math.inf):
        if output_path is None:
            output_path = f"/tmp/{str(uuid.uuid4())}"

        cmd = f"./binary/evtx_dump -f {output_path} -o json {evtx_path}"
        so = os.popen(cmd).read()

        return output_path

    def evtx_to_csv(self,
                    evtx_path: str,
                    output_path: str,
                    nrows: int = math.inf,
                    iterable: bool = False,
                    sep: str = ",",
                    chunck_size: int = 100):

        df = self.evtx_to_df(evtx_path, nrows, iterable=iterable)
        if iterable:
            manager = multiprocessing.Manager()
            columns = manager.list()
            sema = multiprocessing.Semaphore(os.cpu_count() * 2)

            temp_filepath = f"/tmp/{str(uuid.uuid4())}"

            row = next(df)
            columns.extend(set(row.columns))
            row = row.astype({k: pd_dtypes.get(k, "object") for k in row.columns}, errors="ignore")

            row.loc[:, list(columns)].to_csv(temp_filepath, index=False, mode="w", sep=sep, header=None)

            _write_to_csv(df, chunck_size, columns, temp_filepath, sema, sep)

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
        df = pd.json_normalize(input, max_level=None)
        return df.reindex(sorted(df.columns), axis=1)
