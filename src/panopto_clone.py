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
from constants import CACHE_CREATED_FOLDERS, CACHE_FILES_TO_UPLOAD, CACHE_UPLOADED_FILES
from theme import panopto_clone_theme


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

    parser.add_argument(
        "--max-concurrent-tasks",
        dest="max_concurrent_tasks",
        default=5,
        required=False,
        help="How many uploads should occur concurrently.")

    return parser.parse_args()


async def main():
    args = parse_argument()

    progress = Progress(
        "[progress.description]{task.description}",
        "[progress.percentage]{task.percentage:>3.0f}%",
        console=Console(theme=panopto_clone_theme))

    with progress:

        # Before doing anything, check to see if the user want's to clean their cache
        if args.clean:
            # Remove .cache files.
            if os.path.exists(CACHE_CREATED_FOLDERS):
                progress.console.log(f'Deleting {CACHE_CREATED_FOLDERS}', style='info')
                os.remove(CACHE_CREATED_FOLDERS)

            if os.path.exists(CACHE_FILES_TO_UPLOAD):
                progress.console.log(f'Deleting {CACHE_FILES_TO_UPLOAD}', style='info')
                os.remove(CACHE_FILES_TO_UPLOAD)

            if os.path.exists(CACHE_UPLOADED_FILES):
                progress.console.log(f'Deleting {CACHE_UPLOADED_FILES}', style='info')
                os.remove(CACHE_UPLOADED_FILES)

        if args.skip_verify:
            # This line is needed to suppress annoying warning message.
            progress.console.log('SSL verification is off', style='info')
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

        progress.console.log('Authorizing with Panopto', style='info')

        oauth2 = PanoptoOAuth2(args.server, args.client_id, args.client_secret, not args.skip_verify)

        access_token = oauth2.get_access_token_authorization_code_grant()

        progress.console.log('Creating uploader', style='info')

        uploader = PanoptoUploader(args.server, not args.skip_verify, oauth2)

        async with aiohttp.ClientSession() as session:

            # Set the access token
            session.headers.update({'Authorization': 'Bearer ' + access_token})

            # Check to see if folders.cache exists
            if os.path.exists(CACHE_CREATED_FOLDERS):
                progress.console.log('Using directories from cache', style='info')
                created_folders = retrieve_dict_from_disk(CACHE_CREATED_FOLDERS)
            else:
                # Create the directories
                progress.console.log('Creating directories', style='info')
                created_folders = await create_directory_skeleton(
                    source_directory=args.source,
                    uploader=uploader,
                    parent_folder_id=args.destination,
                    session=session,
                    progress=progress
                )
                save_dict_to_disk(created_folders, CACHE_CREATED_FOLDERS)
                progress.console.log('Saved created folders cache', style='info')

            # Get a list of all files that will be uploaded
            tasks = []
            files = [str(file) for file in Path(args.source).rglob('*') if file.is_file()]
            progress.console.log(f'Found {len(files)} videos', style='info')
            write_list_to_file(CACHE_FILES_TO_UPLOAD, files)
            progress.console.log(f'Saved files to upload cache', style='info')

            for file in files:
                task_id = progress.add_task(f'{file}', total=100, visible=False)
                parent_folder = os.path.basename(os.path.dirname(file))
                filtered_dict = {k: v for (k, v) in created_folders.items() if parent_folder in k}
                # This will select the id of the first item in filtered_dict
                if filtered_dict:
                    target_folder_id = next(iter(filtered_dict.values()))['Id']
                else:
                    target_folder_id = args.destination
                    progress.console.log(f'Could not find target_folder_id in filtered_dict. Is filtered_dict empty?', style='danger')

                task = uploader.upload_video_with_progress(
                    folder_id=target_folder_id,
                    session=session,
                    progress=progress,
                    file_path=file,
                    task_id=task_id)
                tasks.append(task)

            progress.console.log(f'Scheduled {len(tasks)} upload tasks', style='info')

            # Function to process tasks in chunks
            async def process_tasks_in_chunks(tasks_to_chunk, chunk_size):
                for i in range(0, len(tasks_to_chunk), chunk_size):
                    chunk = tasks_to_chunk[i:i + chunk_size]
                    await asyncio.gather(*chunk)
                    progress.console.log(f"[grey89]Uploaded {len(chunk)} chunks of files", style='info')

            # Now process tasks in chunks defined by max_concurrent_tasks
            await process_tasks_in_chunks(tasks, int(args.max_concurrent_tasks))


if __name__ == "__main__":
    asyncio.run(main())
