"""Evtx parser
"""

import math
import json
import uuid

import pandas as pd
import numpy as np
import dask.dataframe as dd
from typing import Dict, Iterable, Any, Union, List

from evtx import PyEvtxParser


class EvtxParser:
    """[summary]
    """
    def evtx_to_dask(self, evtx_path: Union[str, List[str]], nrows: int = math.inf, **kwargs) -> dd:
        filepath = f"/tmp/{str(uuid.uuid4())}"
        print(filepath)  # TODO: to delete

        self.evtx_to_csv(evtx_path, filepath, nrows, iterable=True, sep="$")
        dask_df = dd.read_csv(filepath,
                              sep="$",
                              dtype={
                                  'data.Event.EventData.CallTrace': 'object',
                                  'data.Event.EventData.CreationUtcTime': 'object',
                                  'data.Event.EventData.CreationUtcTime ': 'object',
                                  'data.Event.EventData.GrantedAccess': 'object',
                                  'data.Event.EventData.SourceImage': 'object',
                                  'data.Event.EventData.SourceProcessGUID': 'object',
                                  'data.Event.EventData.TargetFilename': 'object',
                                  'data.Event.EventData.TargetFilename ': 'object',
                                  'data.Event.EventData.TargetImage': 'object',
                                  'data.Event.EventData.TargetProcessGUID': 'object',
                                  'data.Event.System.Opcode': int,
                                  'data.Event.System.Version': 'object',
                                  'data.Event.EventData.ProcessId': float,
                                  "data.Event.System.Version": int
                              },
                              **kwargs)
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

    @classmethod
    def _treat_df_for_csv(cls, df):
        df.loc[:, "data.Event.EventData.ProcessId"] = df.loc[:, "data.Event.EventData.ProcessId"].astype('Int32').map(
            lambda x: str(x))
        return df

    def evtx_to_csv(self,
                    evtx_path: str,
                    output_path: str,
                    nrows: int = math.inf,
                    iterable: bool = False,
                    sep: str = ";"):
        df = self.evtx_to_df(evtx_path, nrows, iterable=iterable)
        if iterable:
            temp_filepath = f"/tmp/{str(uuid.uuid4())}"

            row = next(df)
            row = self._treat_df_for_csv(row)
            row.to_csv(temp_filepath, index=False, mode="w", sep=sep, header=None)
            columns = list(row.columns)
            for row in df:
                new_columns = list(set(row.columns) - set(columns))
                old_columns_not_in_df = list(set(columns) - set(row.columns))
                columns = columns + new_columns
                row.loc[:, old_columns_not_in_df] = np.nan

                row = row.loc[:, columns]  # reorder columns
                row = self._treat_df_for_csv(row)
                row.to_csv(temp_filepath, index=False, mode="a", header=None, sep=sep)

            with open(temp_filepath
                      ) as file:  # Need to rewrite the whole file to have the header with all columns in order
                with open(output_path, "w") as outputfile:
                    outputfile.write(f"{sep}".join(columns) + " \n")
                    for row in file:
                        outputfile.write(row)

        else:
            df = self._treat_df_for_csv(df)
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
