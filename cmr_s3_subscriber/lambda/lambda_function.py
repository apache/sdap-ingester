import hashlib
import json
import os
from urllib.parse import urlparse

import boto3
import earthaccess
import requests
import logging

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


def _submit_maap_job(short_name, granule_ur):
    from maap.maap import MAAP

    maap = MAAP()

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

    job = maap.submitJob(
        identifier=f"CMR_subscriber_ingest_{granule_ur}",
        algo_id=algo,
        version=algo_version,
        queue=queue,
        granule_id=granule_ur,
        collection_id=ccid,
        **kwargs
    )

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
            print(f'No record exists for granule {message["granule-ur"]} ({granule_metadata_url}). It was likely '
                  f'superseded and will thus be skipped')
            return
        else:
            raise
    umm = umm_response.json()

    processed_umm = _process_umm(umm)

    print(json.dumps(processed_umm, indent=2))

    if 'MAAP_PGT' in os.environ:
        print('Submitting job through MAAP instead of staging in this function')
        return _submit_maap_job(processed_umm['collection'], message["granule-ur"])

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

    if len(collection_query['Items']) == 0:
        s3_prefix = collection
    else:
        s3_prefix = collection_query['Items'][0]['s3_prefix']['S']

    if s3_prefix[-1] != '/':
        s3_prefix += '/'

    return dict(
        granule=granule,
        collection=collection,
        files=file_map,
        s3_prefix=s3_prefix,
        s3_credentials_url=creds_url
    )


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
