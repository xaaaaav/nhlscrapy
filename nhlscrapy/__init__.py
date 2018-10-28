from datetime import datetime
from io import BytesIO
from gzip import GzipFile
import json

import boto3

s3 = boto3.client('s3')

def _get_start_end_date():
    month = datetime.now().month
    if month >= 9:
        start_date = str(datetime.now().year) + "-09-01"
    elif month >= 1 and month < 9:
        start_date = str(datetime.now().year-1) + "-09-01"
    end_date = datetime.now().strftime("%Y-%m-%d")

    return start_date, end_date

def _generate_years(start_year, end_year):
    year1 = start_year
    year2 = start_year + 1

    while year2 <= end_year:
        season = str(year1) + str(year2)
        yield season
        year1 += 1
        year2 += 1

def _flatten_json(blob, delim="."):
    flattened = {}

    for i in blob.keys():
        if isinstance(blob[i], dict):
            get = _flatten_json(blob[i])
            for j in get.keys():
                flattened[ i + delim + j ] = get[j]
        else:
            flattened[i] = blob[i]

    return flattened

def _validate_years(start_year, end_year):
    if start_year < 1917 or end_year < 1918:
        raise ValueError("Date is before the NHL started recording data.")

    if datetime.now().month >=9:
        if end_year > datetime.now().year + 1 or start_year > datetime.now().year:
            raise ValueError("NHL data not yet recorded.")
    else:
        if end_year > datetime.now().year or start_year >= datetime.now().year:
            raise ValueError("NHL data not yet recorded.")

def _validate_draft_year(year):
	if year < 1995 or year > datetime.now().year:
		raise ValueError("No NHL draft data available")

def _write_to_disk(directory, filename, data, location):
    if not os.path.exists(directory):
        os.makedirs(directory)
    with GzipFile(directory + filename, "w") as gzfile:
        gzfile.write(json.dumps(data).encode("utf-8"))

def _write_to_s3(bucket, path, filename, data):
    gz_body = BytesIO()
    gz = GzipFile(None, 'wb', 9, gz_body)
    gz.write(json.dumps(data).encode('utf-8'))
    gz.close()
    s3.put_object(
        Bucket=bucket,
        Key=path[2:] + filename,
        ContentType='application/json',
        ContentEncoding='gzip',  # MUST have or browsers will error
        Body=gz_body.getvalue()
    )