import argparse
import aiohttp
from panopto_oauth2 import PanoptoOAuth2
from panopto_uploader import PanoptoUploader
from panopto_utils import create_directory_skeleton
from utils import write_list_to_file, retrieve_dict_from_disk, save_dict_to_disk
import urllib3
import asyncio
from pathlib import Path
from rich.console import Console
from rich.progress import Progress
import os
from constants import CACHE_CREATED_FOLDERS, CACHE_FILES_TO_UPLOAD


def parse_argument():
    """
    Argument definition and handling.
    """
    parser = argparse.ArgumentParser(description='Upload a single video file to Panopto server')

    parser.add_argument(
        '--server',
        dest='server',
        required=True,
        help='Server name as FQDN')

    parser.add_argument(
        '--destination',
        dest='destination',
        required=True,
        help='ID of target Panopto folder')

    parser.add_argument(
        '--source',
        dest='source',
        required=True,
        help='Absolute path to source folder')

    parser.add_argument(
        '--client-id',
        dest='client_id',
        required=True,
        help='Client ID of OAuth2 client')

    parser.add_argument(
        '--client-secret',
        dest='client_secret',
        required=True,
        help='Client Secret of OAuth2 client')

    parser.add_argument(
        '--skip-verify',
        dest='skip_verify',
        action='store_true',
        required=False,
        help='Skip SSL certificate verification. (Never apply to the production code)')

    parser.add_argument(
        '--batch-size',
        dest='batch_size',
        required=False,
        help="How many files to sync at a time")

    parser.add_argument(
        "--clean",
        dest="clean",
        action='store_true',
        required=False,
        help="Force removal of .cache files. WARNING: Doing this will likely create duplicate folders.")

    return parser.parse_args()


async def main():
    args = parse_argument()

    # Before doing anything, check to see if the user want's to clean their cache
    if args.clean:
        # Remove .cache files.
        os.remove(CACHE_CREATED_FOLDERS)
        os.remove(CACHE_FILES_TO_UPLOAD)

    if args.skip_verify:
        # This line is needed to suppress annoying warning message.
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    progress = Progress(
        "[progress.description]{task.description}",
        "[progress.percentage]{task.percentage:>3.0f}%",
        console=Console())

    with progress:

        progress.console.log('[grey62]Authorizing')
        oauth2 = PanoptoOAuth2(args.server, args.client_id, args.client_secret, not args.skip_verify)
        access_token = oauth2.get_access_token_authorization_code_grant()

        progress.console.log('[grey62]Creating uploader')

        uploader = PanoptoUploader(args.server, not args.skip_verify, oauth2)

        async with aiohttp.ClientSession() as session:

            # Set the access token
            session.headers.update({'Authorization': 'Bearer ' + access_token})

            # Check to see if folders.cache exists
            if os.path.exists(CACHE_CREATED_FOLDERS):
                created_folders = retrieve_dict_from_disk(CACHE_CREATED_FOLDERS)
            else:
                # Create the directories
                progress.console.log('[grey62]Creating directories')
                created_folders = await create_directory_skeleton(
                    source_directory=args.source,
                    uploader=uploader,
                    parent_folder_id=args.destination,
                    session=session
                )
                save_dict_to_disk(created_folders, CACHE_CREATED_FOLDERS)

            # Get a list of all files that will be uploaded
            tasks = []
            files = [str(file) for file in Path(args.source).rglob('*') if file.is_file()]
            progress.console.log(f'[grey62]Found {len(files)} to upload')
            write_list_to_file(CACHE_FILES_TO_UPLOAD, files)

            for file in files:
                task_id = progress.add_task(f"[blue]{file}", total=100, visible=False)
                parent_folder = os.path.basename(os.path.dirname(file))
                filtered_dict = {k: v for (k, v) in created_folders.items() if parent_folder in k}
                # This will select the id of the first item in filtered_dict
                target_folder_id = next(iter(filtered_dict.values()))['Id']

                task = uploader.upload_video_with_progress(
                    folder_id=target_folder_id,
                    session=session,
                    progress=progress,
                    file_path=file,
                    task_id=task_id)
                tasks.append(task)

            progress.console.log(f'[grey62]Scheduled {len(tasks)} upload tasks')

            # Function to process tasks in chunks
            async def process_tasks_in_chunks(tasks_to_chunk, chunk_size):
                for i in range(0, len(tasks_to_chunk), chunk_size):
                    chunk = tasks_to_chunk[i:i + chunk_size]
                    await asyncio.gather(*chunk)
                    progress.console.log(f"[grey89]Uploaded a chunk of {len(chunk)} files")

            # Now process tasks in chunks of 3
            await process_tasks_in_chunks(tasks, 4)


if __name__ == "__main__":
    asyncio.run(main())
