import argparse
from panopto_oauth2 import PanoptoOAuth2
from panopto_uploader import PanoptoUploader
import urllib3
import os

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


def upload_directory(source_directory, uploader, parent_folder_id=None):

    # First, get the child folders. We will use this to check if folders need to be created or not.
    children = uploader.get_child_folders(folder_id=parent_folder_id)

    for item in os.listdir(source_directory):
        item_path = os.path.join(source_directory, item)

        if os.path.isdir(item_path):

            # If the resource is a directory, check if the directory already exists. If it doesn't, then create it.
            print(f'{item_path} is a directory')

            # Create the folder
            folder = uploader.create_folder(
                folder_id=parent_folder_id,
                folder_name=os.path.basename(item_path),
                folder_description="Created by panopto-clone script")

            # use returned folder.Name and folder.Id

            # Recurse into the directory after creating it in Panopto
            upload_directory(item_path, uploader, folder['Id'])

        elif os.path.isfile(item_path):

            # If the resource is a file, upload it.
            print(f'{item_path} is a file')

            res = uploader.upload_video(file_path=item_path, folder_id=parent_folder_id)

            print(res)


def main():
    args = parse_argument()

    if args.skip_verify:
        # This line is needed to suppress annoying warning message.
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    oauth2 = PanoptoOAuth2(args.server, args.client_id, args.client_secret, not args.skip_verify)

    uploader = PanoptoUploader(args.server, not args.skip_verify, oauth2)

    upload_directory(
        source_directory=args.source,
        parent_folder_id=args.destination,
        uploader=uploader
    )


if __name__ == "__main__":
    main()
