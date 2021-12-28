import re
import math
import json

import pandas as pd
from typing import Dict

from evtx import PyEvtxParser


class evtxJsonParser:
    def evtx_to_df(self, evtx_path: str, nrows: int = math.inf):
        mydict = self.evtx_to_dict(evtx_path, nrows)
        return self.dict_to_df(mydict)

    def evtx_to_dict(self, evtx_path: str, nrows: int = math.inf):
        parser = PyEvtxParser(evtx_path)
        chunck = []

        for i, record in enumerate(parser.records_json()):
            record["data"] = json.loads(record["data"])  # Parsing "data" field as json
            chunck.append(record)

            if i > nrows:
                break

        return chunck

    def dict_to_df(self, input: Dict) -> pd.DataFrame:
        return pd.json_normalize(input, max_level=None)
