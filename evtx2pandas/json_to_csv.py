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
                    chunck_size: int = 500):
        df = self.evtx_to_df(evtx_path, nrows, iterable=iterable)
        if iterable:
            temp_filepath = f"/tmp/{str(uuid.uuid4())}"

            row = next(df)
            row.to_csv(temp_filepath, index=False, mode="w", sep=sep, header=None)
            columns = list(row.columns)

            chuncks = []
            for i, row in enumerate(df):
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
