"""OCR with PDF/TIFF as source files on GCS"""
# USAGE: python text_detect.py SOURCE_FILE OUTPUT_FILE
#   Note that both SOURCE_FILE and OUTPUT_FILE must be
#   in the Google Cloud bucket. For example: 
#
#   python text_detect.py gs://project-name/file.pdf gs://project-name/read
#
# The API will gather the responses for each page into
#   a JSON file on the Google Cloud bucket, e.g.
#   OUTPUT_FILE-output-1-to-1.json.
#   
# This script will then take the recognized text from
#   these JSON files and assemble them into a text document
#   with the name OUTPUT_FILE.txt in the same directory where
#   the script is run.

# Note that you must pass the application credentials
#   so that Google Cloud Vision knows which project 
#   to use:
#   
#   gcloud auth application-default login
#
# Recommended to run in virtualenv.

# This script is based on what Google suggests at
#   https://cloud.google.com/vision/docs/pdf.

import re
import sys
import io
import os
import json
from google.cloud import vision
from google.cloud import storage
from google.protobuf import json_format
from operator import itemgetter
from natsort import natsorted

gcs_source_uri = sys.argv[1]
gcs_destination_uri = sys.argv[2]
local_output_file = os.path.basename(sys.argv[2])

mime_type = 'application/pdf'

batch_size = 1

client = vision.ImageAnnotatorClient()

feature = vision.Feature(
    type_=vision.Feature.Type.DOCUMENT_TEXT_DETECTION)

gcs_source = vision.GcsSource(uri=gcs_source_uri)
input_config = vision.InputConfig(
    gcs_source=gcs_source, mime_type=mime_type)

gcs_destination = vision.GcsDestination(uri=gcs_destination_uri)
output_config = vision.OutputConfig(
    gcs_destination=gcs_destination, batch_size=batch_size)

async_request = vision.AsyncAnnotateFileRequest(
    features=[feature], input_config=input_config,
    output_config=output_config)

operation = client.async_batch_annotate_files(
    requests=[async_request])

print('Waiting for the operation to finish.')
operation.result(timeout=420)

storage_client = storage.Client()

match = re.match(r'gs://([^/]+)/(.+)', gcs_destination_uri)
bucket_name = match.group(1)
prefix = match.group(2)

bucket = storage_client.get_bucket(bucket_name)

response_list = []

blob_list = list(bucket.list_blobs(prefix=prefix))

for blob in blob_list:
    filename = blob.name
    json_string = blob.download_as_text()
    try:
        response = json.loads(json_string)["responses"][0]["fullTextAnnotation"]["text"]
    except KeyError:
        response = ""
    response_list.append([ filename, response ])

sorted_list = natsorted(response_list, key=itemgetter(0))

for item in sorted_list:
    with io.open(local_output_file + '.txt', 'a', encoding='utf8') as outfile:
        outfile.write("""
=== """ + item[0] + """ ==========================
""")
        outfile.write(item[1])