# ExampleIngest

ExampleIngest is a Python library for dealing the ingestion of tv type data. This repo will ingest, 
clean and partition data by day and tv type.    

- It will then clean the data according to the scrubber configurations
- Output the data in a S3 location of your choice 
- Dynamically create DB, and table, specific to tv type  
- Automatically add missing partitions to the table ie// missing day and tv type
- Unclean data will be removed and sent to a quarantine location 


## Setup

1. Create a virtual environment, and install:
```bash
pip install -r requirements.txt
```
2. Set the params in the scrubber config, this will determine the rules for
removing data
3. Make sure you have an S3 bucket containing raw data for tv types / intermediary
S3 loc / output loc
4. Make sure you have set a role on AWS which gives you S3 read/write permissions + 
programmatic athena perms
5. make sure you have AWS CLI latest installed 
6. make sure config and credentials files are set up properly, and you can assume 
role this way 


## Usage
(A)
1. set environment variables in the run/debug configurations
2. run the ingest.py script with any of the functions: setup(), create_table(),
drop_table(), teardown() depending on the needs of the job 
3. If running from scratch then run with setup() 

eg//
```python
from ingest import IngestClass
import os

ingest = IngestClass(region=os.environ.get("REGION"),
                tv_type=os.environ.get("TV"),
                database=os.environ.get("DATABASE"),
                source_bucket=os.environ.get("SOURCE_BUCKET"),
                destination_bucket=os.environ.get("DEST_BUCKET"),
                table=os.environ.get("TABLE"))
ingest.ingest()
```
(B) 
1. run using CLI args, using run.py
2. pass argument from the CLI eg//

```bash
python run.py -tv TCL -de s3://tv-type-output/ -so s3://tv-type-raw/ -p default -t tcl_data -r us-east-1
```



## Contributing
Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change.