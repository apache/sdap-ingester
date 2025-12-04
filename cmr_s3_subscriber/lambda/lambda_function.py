# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


import hashlib
import json
import logging
import os
import re
import traceback
from copy import deepcopy
from functools import cache
from urllib.parse import urlparse

import boto3
import earthaccess
import requests
from shapely import from_wkt, intersects
from shapely.geometry import box

logging.basicConfig(level=logging.DEBUG)

s3 = boto3.client('s3')
sns = boto3.client('sns')
ddb = boto3.client('dynamodb')


SNS_ARN = os.environ['SNS_ARN']
DDB_ARN = os.environ['DDB_ARN']


CHECKSUMS = {
    "MD5": hashlib.md5,
    "SHA-1": hashlib.sha1,
    "SHA-256": hashlib.sha256,
    "SHA-384": hashlib.sha384,
    "SHA-512": hashlib.sha512,
}

GLOBAL = box(-180, -90, 180, 90)


try:
    import requests_mock
except ImportError:
    print('Request mocking disabled')


def _set_edl_environ():
    # Replace with your secret ID
    secret_name = os.environ['SECRET_ARN']

    # Create a Secrets Manager client
    client = boto3.client('secretsmanager')

    # Get the secret value
    get_secret_value_response = client.get_secret_value(SecretId=secret_name)

    # Decode the secret value
    secret_value = get_secret_value_response['SecretString']
    secret_json = json.loads(secret_value)

    os.environ['EARTHDATA_USERNAME'] = secret_json.pop('EARTHDATA_USER')
    os.environ['EARTHDATA_PASSWORD'] = secret_json.pop('EARTHDATA_PASSWORD')

    for secret_k in secret_json:
        os.environ[secret_k] = secret_json[secret_k]


def _process_record(record, bearer_token):
    print(f'Processing record: {record}')

    message_type = record['Type']

    if message_type == 'Notification':
        _handle_cmr_notification(json.loads(record['Message']), bearer_token)
    elif message_type == 'SubscriptionConfirmation':
        _forward_confirmation(record)
    else:
        print(f'Unknown message type: {message_type}')
        _fail_out_record(record, reason='Unknown message type')


def _sns_send(subject, message):
    resp = sns.publish(
        TopicArn=SNS_ARN,
        Subject=subject,
        Message=json.dumps(message, indent=2)[:262144],
    )

    print(resp)


def _forward_confirmation(record):
    print('Forwarding subscription confirmation by SNS')
    _sns_send(
        '[FireAlarm] Confirm forward ingest CMR subscription',
        record
    )


def _fail_out_record(record, reason='exceeded retries'):
    print('Publishing record failure to SNS')
    _sns_send(
        f'[FireAlarm] CMR forward ingest job failed ({reason})',
        record
    )


def _submit_maap_job(short_name, granule_ur, maap_config=None):
    try:
        from maap.maap import MAAP
    except ImportError:
        print('FATAL: MAAP py package not installed. Try rebuilding zip package with the package_maap.zip target')
        raise

    maap = MAAP()

    if maap_config is None:
        raise Exception('MAAP config not provided')

    ccid = maap.searchGranule(
        short_name=short_name,
        readable_granule_name=granule_ur,
        cmr_host="cmr.earthdata.nasa.gov",
        limit=1
    )[0]['collection-concept-id']

    algo = os.environ['MAAP_ALGO_ID']
    algo_version = os.environ['MAAP_ALGO_VERSION']
    queue = os.environ['MAAP_QUEUE']

    kwargs = {v.removeprefix('_maap_kwarg_'): os.environ[v] for v in os.environ.keys() if v.startswith('_maap_kwarg_')}

    kwargs.update(maap_config)

    job_kwargs = dict(
        identifier=f"CMR_subscriber_ingest_{granule_ur}",
        algo_id=algo,
        version=algo_version,
        queue=queue,
        granule_id=granule_ur,
        collection_id=ccid,
        concept_id=ccid,
    )

    final_maap_args = deepcopy(job_kwargs)
    final_maap_args.update(kwargs)

    # print(f'Submitting MAAP job with parameters: {dict(**job_kwargs, **kwargs)}')
    #
    # job = maap.submitJob(
    #     **job_kwargs,
    #     **kwargs
    # )

    print(f'Submitting MAAP job with parameters: {final_maap_args}')

    job = maap.submitJob(**final_maap_args)

    if job.id is None or job.id == '':
        print(f'MAAP job submission failed: {job.error_details}')
        print(job)
        raise Exception('MAAP job submission failed')

    print(f'Submitted job {job.id}')

    return job.id


def _handle_cmr_notification(message, bearer_token):
    print(f'Handling CMR notification: {message}')

    granule_metadata_url = f'{message["location"]}.umm_json'

    print(f'Getting UMM metadata for granule {message["granule-ur"]} from {granule_metadata_url}')

    umm_response = requests.get(granule_metadata_url)
    try:
        umm_response.raise_for_status()
    except:
        if umm_response.status_code == 404:
            print(f'Got a 404 for granule {message["granule-ur"]} at URL {granule_metadata_url}, it was likely '
                  f'superseded. Trying to pull the latest revision.')

            maybe_umm = _maybe_get_latest_revision(message['location'])

            if maybe_umm is None:
                return
            else:
                umm_response = maybe_umm
        else:
            raise

    umm = umm_response.json()
    processed_umm = _process_umm(umm)

    print(json.dumps(processed_umm, indent=2, default=lambda o: repr(o)))

    if processed_umm['collection_geo'] is not None:
        desired_geo = processed_umm['collection_geo']
        granule_geo = processed_umm['spatial_extent']

        if not intersects(granule_geo, desired_geo):
            print(f'Granule {message["granule-ur"]} does not intersect geo filter. Skipping.')
            return

    if 'MAAP_PGT' in os.environ:
        print('Submitting job through MAAP instead of staging in this function')
        return _submit_maap_job(
            processed_umm['collection'],
            message["granule-ur"],
            processed_umm['maap_config']
        )

    print(f'Downloading files for granule {processed_umm["granule"]}')

    s3_client = None

    if processed_umm['s3_credentials_url'] is not None:
        print('Attempting to get temporary S3 credentials from the DAAC...')
        creds = _try_get_s3_creds(processed_umm['s3_credentials_url'], bearer_token)
        if creds is not None:
            print(f'Got temporary S3 credentials: {creds["accessKeyId"]} (exp: {creds["expiration"]})')
            s3_client = boto3.client(
                's3',
                aws_access_key_id=creds['accessKeyId'],
                aws_secret_access_key=creds['secretAccessKey'],
                aws_session_token=creds['sessionToken'],
            )
        else:
            print('Could not get temporary S3 credentials, using HTTP')

    s3_tries = 3 if s3_client is not None else 0

    for file in processed_umm['files']:
        file_info = processed_umm['files'][file]
        dl_path = None

        if s3_tries > 0 and 's3' in file_info:
            dl_path = _try_download_s3(file_info['s3'], s3_client)
            if dl_path is None:
                s3_tries -= 1

        if dl_path is None:
            dl_path = _try_download_http(file_info['http'], bearer_token)

        if dl_path is None:
            raise RuntimeError(f'Could not download file {file}')

        if file_info['checksum'] is not None and file_info['checksum']['Algorithm'] in CHECKSUMS:
            print(f'Verifying checksum [{file_info["checksum"]["Algorithm"]}]...')
            if not _try_validate_checksum(dl_path, file_info['checksum']):
                raise RuntimeError(f'Checksum mismatch for file {file}')
            print('Checksum verified')

        push_s3_client = boto3.client('s3')

        dst_bucket = os.environ['DST_BUCKET']
        dst_key = processed_umm['s3_prefix'] + file

        print(f'Uploading file {file} to s3://{dst_bucket}/{dst_key}')

        push_s3_client.upload_file(
            dl_path,
            dst_bucket,
            dst_key
        )

        os.unlink(dl_path)


def _maybe_get_latest_revision(location_url):
    match = re.search(r'/\d+$', location_url)

    if match is None:
        return None

    latest_rev_url = location_url[:match.start()] + '.umm_json'

    umm_response = requests.get(latest_rev_url)
    try:
        umm_response.raise_for_status()
    except:
        if umm_response.status_code == 404:
            print(f'No record exists for latest revision URL ({latest_rev_url}). It was may no longer exist '
                  f'and will thus be skipped')
            return None
        else:
            raise

    umm = umm_response.json()
    collection = umm['CollectionReference']['ShortName']

    print('Got latest revision metadata, now checking if it is allowed to use this for this collection')

    collection_options = _search_collection_options(collection)

    if collection_options is None:
        print('Skipping this granule because there is no configuration to allow using latest revision metadata')
        return None

    if 'use_latest_rev' not in collection_options:
        print('Skipping this granule because there is no configuration to allow using latest revision metadata')
        return None

    if collection_options['use_latest_rev']['BOOL']:
        print('Collection supports using latest revision metadata, continuing...')
        return umm_response
    else:
        print('Skipping this granule because its collection configuration disallows using latest revision metadata')
        return None


@cache
def _search_collection_options(collection):
    collection_query = ddb.query(
        TableName=DDB_ARN,
        KeyConditionExpression='#collection_short_name = :c',
        ExpressionAttributeValues={
            ':c': {'S': collection}
        },
        ExpressionAttributeNames={
            '#collection_short_name': 'collection'
        }
    )

    print(f'Checked lookup table for {collection}')
    print(collection_query['Items'])

    if len(collection_query['Items']) == 0:
        return None
    else:
        collection_entry = collection_query['Items'][0]
        return collection_entry


def _process_umm(umm):
    # I don't think this is guaranteed (this or EntryTitle). What does other field look like?
    collection = umm['CollectionReference']['ShortName']
    granule = umm['GranuleUR']
    links = umm['RelatedUrls']

    archive_info = umm.get('DataGranule', {}).get('ArchiveAndDistributionInformation', [])

    archive_info = {i['Name']: i['Checksum'] for i in archive_info if 'Checksum' in i}

    http_urls = [u['URL'] for u in links if u['Type'] == 'GET DATA']
    s3_urls = [u['URL'] for u in links if u['Type'] == 'GET DATA VIA DIRECT ACCESS']

    file_map = {}

    for u in http_urls:
        filename = os.path.basename(u)
        file_map.setdefault(filename, {})['http'] = u

    for u in s3_urls:
        filename = os.path.basename(u)
        file_map.setdefault(filename, {})['s3'] = u

    if len(file_map) == 0:
        raise Exception('No data granules found')

    if any('http' not in file_map[f] for f in file_map):
        raise Exception('Granule without HTTP fallback method found')

    for f in file_map:
        if f in archive_info:
            file_map[f]['checksum'] = archive_info[f]
        else:
            file_map[f]['checksum'] = None

    creds_url = None

    for url in links:
        if url['URL'].endswith('/s3credentials'):
            creds_url = url['URL']
            break

    try:
        bounding_rectangles = umm['SpatialExtent']['HorizontalSpatialDomain']['Geometry']['BoundingRectangles']

        if len(bounding_rectangles) > 1:
            raise ValueError('Multiple bounding rectangles given when one expected')

        bbox_dict = bounding_rectangles[0]

        bbox = box(
            bbox_dict['WestBoundingCoordinate'],
            bbox_dict['SouthBoundingCoordinate'],
            bbox_dict['EastBoundingCoordinate'],
            bbox_dict['NorthBoundingCoordinate'],
        )
    except Exception as e:
        print(f'WARN: Unable to get bbox from umm: {e!r}. Using global extent instead')
        bbox = GLOBAL

    collection_entry = _search_collection_options(collection)

    if collection_entry is None:
        s3_prefix = collection
        desired_geo = None
        maap_config = None
    else:
        s3_prefix = collection_entry['s3_prefix']['S'] if 's3_prefix' in collection_entry else collection
        desired_geo = from_wkt(collection_entry['polygon']['S']) if 'polygon' in collection_entry else None
        maap_config = collection_entry['maap_config']['M'] if 'maap_config' in collection_entry else {}
        maap_config = {k: list(v.values())[0] for k, v in maap_config.items()}

    if desired_geo is not None and desired_geo.geom_type != 'Polygon':
        print(f'WARN: Collection settings define incorrect geo filter geometry type. Must be POLYGON. Disabling filter')
        desired_geo = None

    if s3_prefix[-1] != '/':
        s3_prefix += '/'

    return dict(
        granule=granule,
        collection=collection,
        files=file_map,
        s3_prefix=s3_prefix,
        s3_credentials_url=creds_url,
        maap_config=maap_config,
        spatial_extent=bbox,
        collection_geo=desired_geo,
    )


@cache
def _try_get_s3_creds(endpoint, bearer_token):
    try:
        headers = {
            "Authorization": f"Bearer {bearer_token}",
        }

        resp = requests.get(endpoint, headers=headers)
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        print(f'Failed to get S3 credentials [{resp.status_code}]: {e}')
        return None


def _try_download_s3(url, client):
    try:
        parsed_url = urlparse(url)

        bucket = parsed_url.netloc
        key = parsed_url.path.lstrip('/')
        dst = os.path.join('/tmp', os.path.basename(key))

        print(f'Attempting S3 download: {url}')
        client.download_file(bucket, key, dst)
        return dst
    except Exception as e:
        print(f'S3 download failed: {e}')
        return None


def _try_download_http(url, bearer_token):
    headers = {
        "Authorization": f"Bearer {bearer_token}",
    }

    try:
        resp = requests.get(url, headers=headers, stream=True)
        resp.raise_for_status()

        dst = os.path.join('/tmp', os.path.basename(url))

        with open(dst, 'wb') as f:
            for chunk in resp.iter_content(chunk_size=1024 ** 2):
                if chunk:
                    f.write(chunk)
        return dst
    except Exception as e:
        print(f'Failed to download file [{url}]: {e}')
        return None


def _try_validate_checksum(path, checksum):
    hash_fn = CHECKSUMS[checksum['Algorithm']]()

    with open(path, 'rb') as f:
        for chunk in iter(lambda: f.read(1024 * hash_fn.block_size), b''):
            hash_fn.update(chunk)

    dl_hash = hash_fn.hexdigest().lower()

    return dl_hash == checksum['Value'].lower()


def lambda_handler(event, context):
    _set_edl_environ()

    auth = earthaccess.login(strategy='environment')
    bearer_token = auth.token['access_token']

    print(f'Bearer token: {bearer_token}')

    batch_item_failures = []

    for record in event['Records']:
        try:
            _process_record(json.loads(record['body']), bearer_token)
        except Exception as e:
            rec_count = int(record.get('Attributes', {}).get("ApproximateReceiveCount", "5"))

            if rec_count <= 5:
                batch_item_failures.append({"itemIdentifier": record['messageId']})
                print(f'Failed to process record {record["messageId"]} and will retry: {e}')
            else:
                print(f'Failed to process record {record["messageId"]} and will not retry: {e}')
                _fail_out_record(record)

            print(traceback.format_exc())

    # TODO implement
    return {
        'statusCode': 200,
        'batchItemFailures': batch_item_failures
    }


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser()

    parser.add_argument(
        'event_file'
    )

    args = parser.parse_args()

    with open(args.event_file) as f:
        event = json.load(f)

    ret = lambda_handler(event, {})

    print(json.dumps(ret))
