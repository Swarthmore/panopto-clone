import argparse
from panopto_oauth2 import PanoptoOAuth2
from panopto_uploader import PanoptoUploader
import urllib3
import os
import shutil

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


def has_files(directory):
    """
    Check if there are any files in directory.
    """
    for root, _, files in os.walk(directory):
        for file in files:
            return True
    return False



def cleanup(filepath, dest="Processed/"):
    # Ensure dest is created
    if not os.path.exists(dest):
        os.makedirs(dest)

    # Move the file to it's destination
    try:
        shutil.move(filepath, dest)
    except Exception as e:
        print(f'Error: {e}')


def clone(source_directory, uploader, parent_folder_id=None, max_errors = 5):
    """
    Copy a directory to Panopto
    Directories are created in Panopto before the files are uploaded.
    Empty directories are not created.
    """

    error_count = 0

    if error_count <= max_errors:

        for item in os.listdir(source_directory):
            item_path = os.path.join(source_directory, item)

            if os.path.isdir(item_path):

                # Only process if there are files in item_path
                if has_files(item_path):

                    # Create the folder
                    folder = uploader.create_folder(
                        folder_id=parent_folder_id,
                        folder_name=os.path.basename(item_path),
                        folder_description="Created by panopto-clone script")

                    # Recurse into the directory after creating it in Panopto
                    clone(item_path, uploader, folder['Id'])

            elif os.path.isfile(item_path):

                try:
                    # If the resource is a file, upload it.
                    res = uploader.upload_video(file_path=item_path, folder_id=parent_folder_id)

                    # Cleanup the file on the host.
                    cleanup(item_path, "Processed/")

                except Exception as e:
                    # If there's an error, move the file to a folder, Failed/
                    cleanup(item_path, "Failed/")
                    error_count += 1
    else:
        print('Error count reached')


def main():
    args = parse_argument()

    if args.skip_verify:
        # This line is needed to suppress annoying warning message.
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    oauth2 = PanoptoOAuth2(args.server, args.client_id, args.client_secret, not args.skip_verify)

    uploader = PanoptoUploader(args.server, not args.skip_verify, oauth2)

    clone(
        source_directory=args.source,
        parent_folder_id=args.destination,
        uploader=uploader
    )


if __name__ == "__main__":
    main()
