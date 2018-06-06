Workflow_parser
===============
A parser designed to parse logs as request-related events in a distributed system. The parse logic should be defined in *drivers*.
This parser is responsible to parse logs, link distributed events, detect workflow anomalies based on the *driver*, then adjust host clocks, index trace data, visualize performance and even provide an analysis framework based on Jupyter.

Prepare environment
-------------------
```
$git clone https://github.com/cyx1231st/workflow_parser.git
$cd workflow_parser
$sudo ./install.sh
```

Collect traces
--------------
Each set of traces should be collected into logging-alike files under one folder. How files are named is defined by the related *driver*, and usually the name contains information of hostname and/or component name.

Parse traces
------------
User should implement his/her own driver to parse traces collected from target cluster. The driver defines the parse logic modeled by workflow engine, links events that are across components or threads, and feeds runtime variables by parsing filenames and trace strings.

Example drivers are written in `workflow_parser/workflow_parser/drivers/*.py`

The command to parse traces with driver and report results/inconsistencies:
```
$python3 <driver-file> <result-folder>
```

Notes
-----
Currently this is an advanced tool for developers to analyze internal datapath of a distributed system. It's user's responsibility to align his/her analysis intentions with target system logics, tracepoints & trace formats, and parse implementation in the driver. The parser itself cannot know which part is wrong. It can only report inconsistencies between collected traces and driver logics at its best.
