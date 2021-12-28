import math
import json

import pandas as pd
import dask.dataframe as dd
from typing import Dict, Iterable, Any, Union

from evtx import PyEvtxParser


class EvtxParser:
    # def evtx_to_dask_dd(self, evtx_path: str, nrows: int = math.inf) -> dd:
    # pass

    def evtx_to_csv(self, evtx_path: str, output_path: str, nrows: int = math.inf, iterable: bool = False):
        df = self.evtx_to_df(evtx_path, nrows, iterable=iterable)
        df.to_csv(output_path, index=False)

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
