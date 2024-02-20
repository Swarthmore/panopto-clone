import argparse
from panopto_oauth2 import PanoptoOAuth2
from panopto_uploader import PanoptoUploader
from panopto_utils import create_directory_skeleton
import urllib3
import asyncio


def parse_argument():
    """
    Argument definition and handling.
    """
    parser = argparse.ArgumentParser(description='Upload a single video file to Panopto server')
    parser.add_argument('--server', dest='server', required=True, help='Server name as FQDN')
    parser.add_argument('--destination', dest='destination', required=True, help='ID of target Panopto folder')
    parser.add_argument('--source', dest='source', required=True, help='Absolute path to source folder')
    parser.add_argument('--client-id', dest='client_id', required=True, help='Client ID of OAuth2 client')
    parser.add_argument('--client-secret', dest='client_secret', required=True, help='Client Secret of OAuth2 client')
    parser.add_argument('--skip-verify', dest='skip_verify', action='store_true', required=False,
                        help='Skip SSL certificate verification. (Never apply to the production code)')

    return parser.parse_args()


async def main():
    args = parse_argument()

    if args.skip_verify:
        # This line is needed to suppress annoying warning message.
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    oauth2 = PanoptoOAuth2(args.server, args.client_id, args.client_secret, not args.skip_verify)

    uploader = PanoptoUploader(args.server, not args.skip_verify, oauth2)

    await create_directory_skeleton(
        source_directory=args.source,
        uploader=uploader,
        parent_folder_id=args.destination
    )


if __name__ == "__main__":
    asyncio.run(main())
