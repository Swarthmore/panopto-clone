import argparse
from panopto_oauth2 import PanoptoOAuth2
from panopto_uploader import PanoptoUploader
from panopto_utils import create_directory_skeleton
import urllib3
import os
import shutil
import aiohttp
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

#
# def cleanup(filepath, dest="Processed/"):
#     # Ensure dest is created
#     if not os.path.exists(dest):
#         os.makedirs(dest)
#
#     # Move the file to it's destination
#     try:
#         shutil.move(filepath, dest)
#     except Exception as e:
#         print(f'Error: {e}')

#
# def create_directory_skel(source_directory, uploader, parent_folder_id=None):
#     '''
#     Create folders in panopto that match the local tree (empty folders are not created)
#     '''
#     for item in os.listdir(source_directory):
#         item_path = os.path.join(source_directory, item)
#
#         if os.path.isdir(item_path):
#
#             # Only process if there are files in item_path
#             if has_files(item_path):
#                 # Create the folder
#                 folder = uploader.create_folder(
#                     folder_id=parent_folder_id,
#                     folder_name=os.path.basename(item_path),
#                     folder_description="Created by panopto_clone.py")
#
#                 # Recurse into the directory after creating it in Panopto
#                 create_directory_skel(item_path, uploader, folder['Id'])


#        elif os.path.isfile(item_path):
#
#            try:
#                # If the resource is a file, upload it.
#                res = uploader.upload_video(file_path=item_path, folder_id=parent_folder_id)
#
#                # Cleanup the file on the host.
#                cleanup(item_path, "Processed/")
#
#            except Exception as e:
#                # If there's an error, move the file to a folder, Failed/
#                cleanup(item_path, "Failed/")
#                error_count += 1
#    else:
#        print('Error count reached')


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

    # async with aiohttp.ClientSession() as session:
    #
    #
    #     await clone(
    #         source_directory=args.source,
    #         parent_folder_id=args.destination,
    #         uploader=uploader
    #     )


if __name__ == "__main__":
    asyncio.run(main())
