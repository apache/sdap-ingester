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
import requests
import earthaccess
import yaml
import os


def parse_args():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        'config',
        default='config.yaml',
        help='Configuration file'
    )

    parser.add_argument(
        'ccid',
        help='Collection concept ID to subscribe to'
    )

    parser.add_argument(
        '-q', '--queue',
        default=None,
        help='Subscriber queue ARN',
        required=False,
        dest='queue'
    )

    parser.add_argument(
        '--dir',
        default='subscriptions',
        help='Directory where CMR subscription XML responses are stored'
    )

    parser.add_argument(
        '--dryrun',
        action='store_true',
        help='Do not make CMR API calls except for auth'
    )

    return parser.parse_args()


def main(args):
    with open(args.config, 'r') as f:
        config = yaml.safe_load(f)

    os.environ['EARTHDATA_USERNAME'] = config['edl_username']
    os.environ['EARTHDATA_PASSWORD'] = config['edl_password']

    auth = earthaccess.login(strategy='environment')
    bearer_token = auth.token['access_token']

    if args.queue is None:
        queue_arn = config['queue_arn']
    else:
        queue_arn = args.queue

    ccid = args.ccid
    os.makedirs(args.dir, exist_ok=True)

    subscription_request = {
        "Name": f"{config['edl_username']}-{ccid}-subscription",
        "CollectionConceptId": ccid,
        "Type": "granule",
        "Query": "*",
        "EndPoint": queue_arn,
        "Mode": ["New", "Update"],
        "Method": "ingest",
        "MetadataSpecification": {
            "URL": "https://cdn.earthdata.nasa.gov/umm/subscription/v1.1.1",
            "Name": "UMM-Sub",
            "Version": "1.1.1"
        }
    }

    headers = {
        "Authorization": f"Bearer {bearer_token}",
        "Content-Type": "application/vnd.nasa.cmr.umm+json"
    }

    if args.dryrun:
        print("Would issue the following POST request to https://cmr.earthdata.nasa.gov/ingest/subscriptions/")
        print(subscription_request)
        print(f'{headers=}')
        exit()

    try:
        response = requests.post(
            "https://cmr.earthdata.nasa.gov/ingest/subscriptions/",
            headers=headers,
            json=subscription_request
        )

        if response.ok:
            print("Successfully created CMR subscription")
            print(f"Response: {response.text}")
            with open(
                    os.path.join(args.dir, f'{ccid}-subscription.xml'),
                    'w'
            ) as f:
                f.write(response.text)
        else:
            print(f"Error creating subscription. Status code: {response.status_code}")
            print(f"Response: {response.text}")

    except Exception as e:
        print(f"Error making subscription request: {str(e)}")


if __name__ == '__main__':
    main(parse_args())
