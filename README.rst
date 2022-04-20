.. contents ::

Introduction
------------
Convert EVTX (Log file created by the Windows 7 Event Viewer) to pandas, but also CSV, JSON or Dask DataFrame.

JSON creation is based on: https://github.com/omerbenamram/evtx

Installation
------------

::

   pip install evtx2pandas


Usage
------------

::

   from evtx2pandas.json_to_csv import EvtxParser
   
   reader = EvtxParser()

   # To convert evtx to pandas DataFrame
   df = reader.evtx_to_df(evtx_path)

   # To convert evtx to Dask DataFrame
   dask_dd = reader.evtx_to_dask(json_path)

   # To write evtx as json
   reader.evtx_to_json(json_path, output_path=temp_file)

   mydict = json.load(open(temp_file)) # To read the JSON output as python dict

   # To write evtx as CSV
   reader.evtx_to_csv(json_path, output_path=temp_file)

   df = pd.read_csv(temp_file, sep=";") # To read the CSV output as padnas DataFrame

License
-------

evtx2pandas is free software; you can redistribute it and/or modify it
under the terms of the GNU General Public License as published by the Free
Software Foundation; either version 2 of the License, or (at your option)
any later version.

See the file COPYING for the full text of GNU General Public License version 2.
