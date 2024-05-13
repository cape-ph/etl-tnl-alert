"""ETL script for raw Epi/HAI Tennessee health lab alert xlsx."""

import io
import re
import sys

import boto3 as boto3
import dateutil.parser as dparser
import pandas as pd
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from docx import Document
from pyspark.context import SparkContext

# for our purposes here, the spark and glue context are only (currently) needed
# to get the logger.
spark_ctx = SparkContext()
glue_ctx = GlueContext(spark_ctx)
logger = glue_ctx.get_logger()

# TODO:
#   - add error handling for the format of the document being incorrect
#   - figure out how we want to name and namespace clean files (e.g. will we
#     take the object key we're given, strip the extension and replace it with
#     one for the new format, or will we do something else)
#   - see what we can extract out of here to be useful for other ETLs. imagine
#     we'd have a few different things that could be made into a reusable
#     package

OUT_COL_NAMES = [
    "Empty",
    "Testing-Results",
    "State",
    "Date_Received",
    "Date_Reported",
    "Last_Name",
    "First_Name",
    "DOB",
    "State_Lab_ID",
    "Specimen",
    "Date_Collection",
    "Submitting_Facility",
    "Sample_Collection_Facility",
    "Known_Positive",
    "Colonization_Detected",
]

parameters = getResolvedOptions(
    sys.argv,
    [
        "RAW_BUCKET_NAME",
        "ALERT_OBJ_KEY",
        "CLEAN_BUCKET_NAME",
    ],
)

raw_bucket_name = parameters["RAW_BUCKET_NAME"]
alert_obj_key = parameters["ALERT_OBJ_KEY"]
clean_bucket_name = parameters["CLEAN_BUCKET_NAME"]

# NOTE: for now we'll take the alert object key and change out the file
#       extension for the clean data (leaving all namespacing and such). this
#       will probably need to change
clean_obj_key = alert_obj_key.replace(".xlsx", ".csv")

# NOTE: May need some creds here
s3_client = boto3.client("s3")

# try to get the docx object from S3 and handle any error that would keep us
# from continuing.
response = s3_client.get_object(Bucket=raw_bucket_name, Key=alert_obj_key)

status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")

if status != 200:
    err = (
        f"ERROR - Could not get object {alert_obj_key} from bucket "
        f"{raw_bucket_name}. ETL Cannot continue."
    )

    logger.error(err)

    # NOTE: need to properly handle exception stuff here, and we probably want
    #       this going somewhere very visible (e.g. SNS topic or a perpetual log
    #       as someone will need to be made aware)
    raise Exception(err)

logger.info(f"Obtained object {alert_obj_key} from bucket {raw_bucket_name}.")

# handle the document itself...

# NOTE: The data in this spreadsheet can take a few different forms based on
#       pathogen. This could change in the future. The speadsheet (regardless
#       of pathogen) does have the same general format and is assumed to
#       contain a single worksheet with a table that needs to be processed,
#       and nothing else. The table consists of:
#       - a 1 column header row that can be ignored
#       - another header row that contains all the column names for the table
#       - rows of data
data = pd.read_excel(response.get("Body"))

# strip out the ingorable header and reset the index
data = data[1:].reset_index(drop=True)

data.columns = OUT_COL_NAMES

# Output Column Creation
interim = pd.DataFrame()

# TODO: Right now we know of 2 formats the data in these alerts can take on
#       based on organism being alerted about (really, just the data in the
#       column `Description of Testing Completed and Results (including
#       organism name)`). There is one format for Candidia auris and one
#       format for all others we know of right now. The processing needs will
#       be slightly different for this cell and we need a way to know which
#       format to parse. Ideally we'd work with the producers of the alerts
#       and see if we can get all alerts put into a similar format, but for
#       right now we'll check for the substring 'Candida auris' and react
#       based on that.

if data.find("Candida auris"):
    # get a list of pairs of (mechanism, organism) from manipulating the incoming
    # data column
    pairs = [
        (m.strip(), o.strip())
        for m, o in list(map(lambda x: x.split("-"), data["Testing-Results"]))
    ]
    # extract the lists and put them in the right places
    interim["Mechanism (*Submitters Report)"], interim["Organism"] = zip(*pairs)
else:
    # pattern for the organism part
    # NOTE: This is brittle. It banks on the organism always being the format
    #      `X. whatevs ,`, so if the origanism is ever fully spelled out or
    #      anything, this will break. We need to see if we can get a common
    #      format for everything in this first column.
    org_pattern = r"([A-Z]\..*?)(?=,)"
    # compiled pattern so we only compile once
    comp_pattern = re.compile(org_pattern)
    pairs = [
        (
            re.sub(org_pattern, lambda x: "", s),
            comp_pattern.search(s).group(0) or "UNKNOWN",
        )
        for s in data["Testing-Results"]
    ]
    # extract the lists and put them in the right places
    interim["Mechanism (*Submitters Report)"], interim["Organism"] = zip(*pairs)
interim["Date Received"] = pd.to_datetime(data["Date_Received"], errors="coerce")
interim["Date Reported"] = pd.to_datetime(data["Date_Reported"], errors="coerce")
interim["Patient Name"] = (
    data["Last_Name"].str.title() + ", " + data["First_Name"].str.title()
)
interim["DOB"] = pd.to_datetime(data["DOB"], errors="coerce")
interim["Source"] = data["Specimen"].str.capitalize()
interim["Date of Collection"] = pd.to_datetime(data["Date_Collection"], errors="coerce")
interim["Testing Lab"] = "TNL"


####################


# NOTE: this document is assumed to contain a single table that needs to be
#       processed and nothing else. The file consists of:
#       - a 2 column header row that contains a column (0 index) with the alert
#         report date, which we need (rest of this row can be ignored)
#       - another header row that contains all the column names for the table
#       - rows of data
document = Document(response.get("Body"))

table = document.tables[0]
data = [[cell.text for cell in row.cells] for row in table.rows]
data = pd.DataFrame(data)

# grab the alert report date and then drop that row
date_received = pd.to_datetime(dparser.parse(data.iloc[0, 0], fuzzy=True))
data.columns = data.loc[1]
data = data[2:].reset_index(drop=True)

# now perform the ETL on the data rows
# NOTE: Questions about the data:
#           - Do we need to split this name to better enable queries later?
#           - will the name only ever be composed of first and last (i.e. no
#             middle name handling)?
#           - do we not want the lab id to carry over into clean data? same
#             with organism id (and anything else that doesn't carry over
#             currently). there's very little penalty (storage cost) for it
#             carrying over, and if it's not part of the AR log, we just don't
#             include it in the AR query, but we still have it if we end up
#             needing it for anything else
interim = pd.DataFrame()
interim["Mechanism (*Submitters Report)"] = data["Anti-Microbial Resistance RT-PCR"]
interim["Organism"] = data["Organism ID"]
interim["Date Received"] = date_received
interim["Date Reported"] = date_received
interim["Patient Name"] = data["Patient Name"]
interim["DOB"] = pd.to_datetime(data["DOB"], errors="coerce")
interim["Source"] = data["Source"].str.capitalize()
interim["Date of Collection"] = pd.to_datetime(
    data["Date of Collection"], errors="coerce"
)
interim["Testing Lab"] = "GPHL"
interim["Facility of Origin"] = data["Received from"]

# write out the transofrmed data
with io.StringIO() as csv_buff:
    interim.to_csv(csv_buff, index=False)

    response = s3_client.put_object(
        Bucket=clean_bucket_name, Key=clean_obj_key, Body=csv_buff.getvalue()
    )

    status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")

    if status != 200:
        err = (
            f"ERROR - Could not write transformed data object {clean_obj_key} "
            f"to bucket {clean_bucket_name}. ETL Cannot continue."
        )

        logger.error(err)

        # NOTE: need to properly handle exception stuff here, and we probably
        #       want this going somewhere very visible (e.g. SNS topic or a
        #       perpetual log as someone will need to be made aware)
        raise Exception(err)

    logger.info(
        f"Transformed {raw_bucket_name}/{alert_obj_key} and wrote result "
        f"to {clean_bucket_name}/{clean_obj_key}"
    )
