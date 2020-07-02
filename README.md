# Trip Tracker

## Description
- Reads the `GPX file` of stored waypoints data, processes it and ingests the data
to the `Elasticsearch` index.
- If there are gaps in the waypoints sequence, the sequence can be corrected
with using a reference `GPX file` that would fill the gaps.
- Also `untracked` trips can be ingested if the `start` datetime and `end`
datetime are known.
- The script can be run without ingesting the data to the `Elasticsearch` index, 
which would then only provide aggregated information of the trip.
- If the data are ingested to the `Elasticsearch` index then the trip trajectory
can be visualized in the `Kibana` maps visualization.
- Automatic detection of inactivity, i.e. no movement

## Trip types definitions:
- `driven` - trip type of a trip that was tracked by an application that provides the `GPX
file` with these data: `latitude`, `longitude`, `elevation` and `timestamp`
- `untracked` - trip type of a trip that was not tracked by the application but
was executed by the user and the user provided `start` of the trip and `end` of the trip 
datetime timestamps. The user created the `GPX file` after the trip. The `GPX file`
contains these data: `latitude`, `longitude`, `elevation`
- `planned` - trip type of a trip that has not been yet executed, the user
only has planned the trip. The `GPX file` contains these data: `latitude`, `longitude`, `elevation`

## Prerequisities
- The `virtualenv` is installed
- Supported for Python 3, developed and tested with Python 3.7.4 

## Installation
**1.Create virtual environment**

```
python -m venv .venv
.venv/Scripts/activate
pip install -r reqs.txt
```
**2.Setup Elasticsearch**
 
Please refer to the official sites of `Elasticsearch` and `Kibana` for installation
and index creation guides.

## Usage
```
usage: trip_tracker.py [-h] --mode MODE --gpx-file GPX_FILE
                       [--ref-file REF_FILE] [--index INDEX] [--start START]
                       [--end END]

optional arguments:
  -h, --help           show this help message and exit
  --mode MODE          Mean of transport, bike, run or walk
  --gpx-file GPX_FILE  path to the file to be processed
  --ref-file REF_FILE  path to the reference file to be used for correction
  --index INDEX        elasticsearch index to be used for data storage, if None, no indexing will happen
  --start START        Isoformat time of a trip start, set it for untracked trips, None for tracked or planned trips
  --end END            Isoformat time of a trip end, set it for untracked trips, None for tracked or planned trips
```