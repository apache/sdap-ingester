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

    sub_id = parser.add_mutually_exclusive_group(required=True)

    sub_id.add_argument(
        '--native_id',
        help='Native ID to delete'
    )

    sub_id.add_argument(
        '--response-xml',
        help='XML file from subscribe.py from which to get native ID to delete'
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

    if args.native_id is not None:
        nid = args.native_id
    else:
        from xml.etree import ElementTree as ET

        tree = ET.parse(args.response_xml)
        root = tree.getroot()
        nid = root.find('native-id').text

    headers = {
        "Authorization": f"Bearer {bearer_token}",
    }

    if args.dryrun:
        print(f"Would issue a DELETE request to https://cmr.earthdata.nasa.gov/ingest/subscriptions/{nid}")
        print(f'{headers=}')
        exit()

    try:
        response = requests.delete(
            f"https://cmr.earthdata.nasa.gov/ingest/subscriptions/{nid}",
            headers=headers,
        )

        if response.ok:
            print("Successfully deleted CMR subscription")
            print(f"Response: {response.text}")

            if args.response_xml is not None:
                try:
                    os.unlink(args.response_xml)
                except:
                    print('Could not delete XML file')
        else:
            print(f"Error deleting subscription. Status code: {response.status_code}")
            print(f"Response: {response.text}")

    except Exception as e:
        print(f"Error making subscription request: {str(e)}")


if __name__ == '__main__':
    main(parse_args())
