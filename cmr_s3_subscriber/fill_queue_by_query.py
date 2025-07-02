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


import argparse
import json
import os
from datetime import datetime
from urllib.parse import urlparse
from uuid import uuid4

import boto3
import earthaccess
import requests
import yaml

CMR_MIN_PAGE = 10
CMR_MAX_PAGE = 2000


def parse_arguments():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        'config',
        default='config.yaml',
        help='Configuration file for Earthdata login and SQS ARN'
    )

    parser.add_argument(
        'ccid',
        help='Collection concept ID to fill data for'
    )

    parser.add_argument(
        '--start-time',
        required=False,
        default=None,
        type=lambda x: datetime.strptime(x, '%Y-%m-%dT%H:%M:%SZ'),
        help='Optional filter collection granules to those at or after this time. Must be in yyyy-mm-ddThh:mm:ssZ '
             'format'
    )

    parser.add_argument(
        '--end-time',
        required=False,
        default=None,
        type=lambda x: datetime.strptime(x, '%Y-%m-%dT%H:%M:%SZ'),
        help='Optional filter collection granules to those at or before this time. Must be in yyyy-mm-ddThh:mm:ssZ '
             'format'
    )

    def __validate_bbox(s):
        coords = [float(c) for c in s.split(',')]

        if len(coords) != 4:
            raise ValueError(f'Expected 4 coordinates but got {len(coords)}')

        if not (-180 <= coords[0] <= 180 and -180 <= coords[2] <= 180):
            raise ValueError('A longitude coordinate is out of range')

        if not (-90 <= coords[1] <= 90 and -90 <= coords[3] <= 90):
            raise ValueError('A latitude coordinate is out of range')

        if coords[2] <= coords[0] or coords[3] <= coords[1]:
            raise ValueError('One of min/max lat/lon are flipped')

        return s

    parser.add_argument(
        '--bbox',
        required=False,
        type=__validate_bbox,
        default=None,
        help='Optional filter to filter collection granule to those matching a bounding box. Defined as 4 '
             'comma-separated numbers in order of: '
             'minimum_longitude,minimum_latitude,maximum_longitude,maximum_latitude. Longitudes must be between +/-180 '
             'and latitudes must be between +/- 90'
    )

    def __validate_s3_url(s):
        parsed = urlparse(s)

        if parsed.scheme != 's3':
            raise ValueError(f'Expected S3 URL but got {parsed.scheme}')

        return s

    parser.add_argument(
        '--staged-data',
        required=False,
        default=None,
        type=__validate_s3_url,
        help='S3 URL prefix for location of data already staged. If provided, granules with all data files already '
             'staged in S3 will be filtered out'
    )

    def __positive_integer(s):
        i = int(s)

        if i <= 0:
            raise ValueError(f'Expected positive integer but got {i}')

        return i

    parser.add_argument(
        '--limit',
        required=False,
        default=None,
        type=__positive_integer,
        help='Limit the number of generated SQS messages'
    )

    parser.add_argument(
        '--page-size',
        required=False,
        default=1000,
        type=__positive_integer,
        help='Page size for CMR queries. Default: 1000, min: 10, max: 2000. Values outside the min/max will be '
             'overridden'
    )

    return parser.parse_args()


def filter_granules(granules, s3_url):
    parsed_url = urlparse(s3_url)
    s3 = boto3.client('s3')

    paginator = s3.get_paginator('list_objects_v2')

    staged_files = []

    for page in paginator.paginate(Bucket=parsed_url.netloc, Prefix=parsed_url.path.lstrip('/')):
        staged_files.extend([os.path.basename(o['Key']) for o in page.get('Contents', [])])

    staged_files = set(staged_files)

    print(f'Found {len(staged_files):,} files staged in S3')

    filtered_granules = []

    for granule in granules:
        links = granule['umm']['RelatedUrls']

        data_urls = [u['URL'] for u in links if u['Type'] == 'GET DATA']
        data_files = set([os.path.basename(url) for url in data_urls])

        if data_files.intersection(staged_files) != data_files:
            filtered_granules.append(granule)

    print(f'Filtered {len(granules):,} granules to {len(filtered_granules):,}')
    return filtered_granules


def main(args):
    with open(args.config, 'r') as f:
        config = yaml.safe_load(f)

    os.environ['EARTHDATA_USERNAME'] = config['edl_username']
    os.environ['EARTHDATA_PASSWORD'] = config['edl_password']

    auth = earthaccess.login(strategy='environment')
    bearer_token = auth.token['access_token']

    queue_arn = config['queue_arn']

    ccid = args.ccid

    cmr_search_url = 'https://cmr.earthdata.nasa.gov/search/granules.umm_json_v1_4'

    search_query = {
        'collection_concept_id': ccid,
        'page_size': min(max(args.page_size, CMR_MIN_PAGE), CMR_MAX_PAGE)
    }

    if args.start_time is not None or args.end_time is not None:
        start_q_str = args.start_time.strftime('%Y-%m-%dT%H:%M:%SZ') if args.start_time is not None else ''
        end_q_str = args.end_time.strftime('%Y-%m-%dT%H:%M:%SZ') if args.end_time is not None else ''

        search_query['temporal[]'] = f'{start_q_str},{end_q_str}'

    if args.bbox is not None:
        search_query['bounding_box[]'] = args.bbox

    matched_granules = []

    print(f'Querying {cmr_search_url} with params {search_query}')
    response = requests.get(cmr_search_url, params=search_query)
    response.raise_for_status()

    response_json = response.json()

    n_hits = response_json['hits']
    matched_granules.extend(response_json['items'])
    search_after = response.headers.get('CMR-Search-After', None)

    while search_after is not None:
        headers = {'CMR-Search-After': search_after}
        print(f'Querying {cmr_search_url} with params {search_query} and headers {headers}')
        response = requests.get(cmr_search_url, params=search_query, headers=headers)
        response.raise_for_status()

        response_json = response.json()

        matched_granules.extend(response_json['items'])
        search_after = response.headers.get('CMR-Search-After', None)

    print(f'Finished CMR query. Found {len(matched_granules):,} granules.')

    if len(matched_granules) != n_hits:
        print('Mismatch between number of granules and initial number of hits')
        exit(1)

    print(json.dumps(matched_granules[0], indent=2))

    if args.staged_data is not None:
        matched_granules = filter_granules(matched_granules, args.staged_data)

    if args.limit is not None:
        print(f'Limiting {len(matched_granules)} granules to {args.limit}')
        matched_granules = matched_granules[:args.limit]

    print('Converting to CMR notifications')

    def _try_get_pgid_from_umm(umm):
        try:
            identifiers = umm['DataGranule']['Identifiers']
            for i in identifiers:
                if i['IdentifierType'] == 'ProducerGranuleId':
                    return i['Identifier']
            return umm['GranuleUR']
        except:
            return umm['GranuleUR']

    matched_granules = [
        {
            'concept-id': m['meta']['concept-id'],
            'granule-ur': m['umm']['GranuleUR'],
            'location': f"https://cmr.earthdata.nasa.gov:443/concepts/"
                        f"{m['meta']['concept-id']}/{m['meta']['revision-id']}",
            'producer-granule-id': _try_get_pgid_from_umm(m['umm'])
        } for m in matched_granules
    ]

    print(json.dumps(matched_granules[0], indent=2))

    print('Converting to SQS messages')

    matched_granules = [
        {
            'Type': 'Notification',
            'MessageId': str(uuid4()),
            'TopicArn': 'arn:aws:sns:us-east-1:621933553860:cmr-subscriptions-prod',
            'Subject': 'New Notification',
            'Message': json.dumps(m),
            'Timestamp': datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ'),
            'MessageAttributes': {
                'collection-concept-id': {
                    'Type': 'String',
                    'Value': ccid
                },
                'mode': {
                    'Type': 'String',
                    'Value': 'New'
                },
                'subscriber': {
                    'Type': 'String',
                    'Value': config['edl_username']
                }
            }

        } for m in matched_granules
    ]

    print(json.dumps(matched_granules[0], indent=2))

    sqs = boto3.client('sqs')

    queue_url = sqs.get_queue_url(QueueName=queue_arn.split(':')[-1])['QueueUrl']

    message_batches = [[{
        'Id': str(uuid4()),
        'MessageBody': json.dumps(m),
    } for m in matched_granules[i:i+10]] for i in range(0, len(matched_granules), 10)]

    failed = []

    for batch in message_batches:
        print(f'Sending batch of {len(batch)} messages to queue {queue_url}')
        response = sqs.send_message_batch(QueueUrl=queue_url, Entries=batch)
        failed.extend(response.get('Failed', []))

    if len(failed) > 0:
        print(f'Failed {len(failed):,} failed messages:\n{json.dumps(failed, indent=2)}')


if __name__ == '__main__':
    main(parse_arguments())
    # filter_granules([], 's3://aqacf-nexus-stage/TEMPO/TEMPO_NO2_L3_V03/')

