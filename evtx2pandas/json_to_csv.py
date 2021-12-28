import re
import math
import json

import pandas as pd
from typing import Dict, Iterable, Any

from evtx import PyEvtxParser


class EvtxJsonParser:
    def evtx_to_df(self, evtx_path: str, nrows: int = math.inf) -> pd.DataFrame:
        mydict = self.evtx_to_dict(evtx_path, nrows)
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
