import asyncio
import codecs
import copy
import os
import random
import time
from datetime import datetime

import aioboto3
import aiohttp
from boto3.s3.transfer import S3TransferConfig
from botocore.config import Config

from constants import CACHE_UPLOADED_FILES
from constants import MAX_PROCESSING_POLL_TIME, PART_SIZE, DELAY
from utils import bytes_to_megabytes


class PanoptoUploader:
    def __init__(self, server, ssl_verify, oauth2):
        """
        Constructor of uploader instance.
        This goes through authorization step of the target server.
        """
        self.server = server
        self.ssl_verify = ssl_verify
        self.oauth2 = oauth2

    def __setup_or_refresh_access_token(self, session):
        """
        This method invokes OAuth2 Authorization Code Grant authorization flow.
        It goes through browser UI for the first time.
        It refreshes the access token after that and no user interfaction is requetsed.
        This is called at the initialization of the class, as well as when 401 (Unaurhotized) is returend.
        """
        access_token = self.oauth2.get_access_token_authorization_code_grant()
        session.headers.update({'Authorization': 'Bearer ' + access_token})

    async def __inspect_response_is_retry_needed(self, session, response, update_progress):
        """
        Inspect the response of an aiohttp call.
        True indicates the retry needed, False indicates success. Otherwise, an exception is thrown.

        This method detects 403 (Forbidden), refreshes the access token, and returns as 'is retry needed'.
        """
        if response.status == 200 or response.status == 201:
            # Success on 2xx response.
            return False

        if response.status == 400:
            # the request is bad
            update_progress('[bold][red]Bad request.[/red][/bold]')
            return False

        # 401 - The user is not authorized to perform the requested action
        # 403 - User does not have permission to access this function
        if response.status == 401 or response.status == 403:
            update_progress(
                '[bold][yellow]Forbidden. This may mean token expired. Refreshing access token.[/yellow][/bold]')
            self.__setup_or_refresh_access_token(session)  # Ensure this method is async and awaits the token refresh
            return True

        # For aiohttp, use response.raise_for_status() to automatically throw if the status is an error code
        # Make sure to use it where it doesn't preempt your checks for recoverable error codes like 403
        try:
            update_progress(f'[bold][red]Received response status {response.status}[/red][/bold]')
            response.raise_for_status()
        except aiohttp.ClientError as e:
            # Handle specific aiohttp exceptions or re-raise
            # This is where you might log the error or handle specific HTTP errors differently
            update_progress(f'[bold][red]{e}[/red][/bold]')
            raise e

        # If you've gotten here, it means neither 200 nor 403 status, and raise_for_status didn't trigger an exception
        # This line might be redundant due to raise_for_status above, but included for clarity
        return False

    async def create_folder(self, folder_name, folder_id, session, folder_description=None):
        """
        Create a folder in Panopto
        """
        try:
            url = f'https://{self.server}/Panopto/api/v1/folders'

            payload = {
                'Name': folder_name,
                'Description': folder_description,
                'Parent': folder_id}

            res = await session.post(
                url,
                json=payload,
                ssl=self.ssl_verify)

            if res.status == 200:
                return await res.json()
            else:
                return False

        except aiohttp.ClientResponseError as e:
            # Handle client response errors (e.g., 404, 403, 500)
            print(f'HTTP Error: {e}')
        except aiohttp.ClientError as e:
            # Handle broader aiohttp client errors
            print(f'Aiohttp Error: {e}')
        except Exception as e:
            # Handle other errors (e.g., from JSON parsing)
            print(f'Unexpected Error: {e}')

    async def get_child_folders(self, folder_id, session, page_number=0, sort_order="Desc", sort_field="Name"):
        """
        Get a list of child folders from the given parent
        """
        try:
            url = f'https://{self.server}/Panopto/api/v1/folders/{folder_id}/children?sortField={sort_field}&sortOrder={sort_order}&pageNumber={page_number}'

            res = await session.get(
                url,
                ssl=self.ssl_verify)

            return await res.json()

        except aiohttp.ClientResponseError as e:
            # Handle client response errors (e.g., 404, 403, 500)
            print(f'HTTP Error: {e}')
        except aiohttp.ClientError as e:
            # Handle broader aiohttp client errors
            print(f'Aiohttp Error: {e}')
        except Exception as e:
            # Handle other errors (e.g., from JSON parsing)
            print(f'Unexpected Error: {e}')

    async def upload_video_with_progress(self, session, folder_id, file_path, progress, task_id, task_color, manifest):

        def update_progress(msg, completed=None):
            # prefix the task id to the message
            log_msg = f'[bold][{task_color}][{task_id}][/bold][/{task_color}] [dim]{msg}[/dim]'
            progress.console.log(log_msg)
            progress.update(
                task_id,
                visible=True,
                completed=completed
            )
            # Optionally, update the task's progress bar here
            if completed:
                progress.update(
                    task_id,
                    completed=completed
                )

        await self.upload_video(session=session, file_path=file_path, folder_id=folder_id, progress=progress,
                                task_id=task_id, update_progress=update_progress, manifest=manifest)

        # Write the file path, folder location, and other stats to disk
        try:
            with open(CACHE_UPLOADED_FILES, 'a', encoding='utf-8') as file:
                line = f'"{task_id}","{file_path}","{folder_id}"\n'
                file.write(line)
        except FileNotFoundError:
            # Handle case when the cache file does not exist
            with open(CACHE_UPLOADED_FILES, 'w', encoding='utf-8') as file:
                # Write the header line before anything else
                lines = [
                    '"task_id","file_path","folder_id"\n',
                    f'"{task_id}","{file_path}","{folder_id}"\n'
                ]
                file.writelines(lines)

        update_progress(f'[bold][green]Finished uploading[/green][/bold]')

        # Mark the task as completed, and hide it
        progress.update(
            task_id,
            completed=True,
            visible=False
        )

    async def upload_video(self, session, file_path, folder_id, progress, task_id, update_progress, manifest):
        """
        Main upload method to go through all required steps.
        """

        TOTAL_STEPS = 6

        # step 1 - Create a session
        update_progress("Creating session")
        session_upload = await self.__create_session(session=session, folder_id=folder_id,
                                                     update_progress=update_progress)
        upload_id = session_upload['ID']
        upload_target = session_upload['UploadTarget']
        update_progress('Finished creating session', completed=1)

        # step 2 - upload the video file
        update_progress("Uploading file")
        await self.__multipart_upload_single_file_with_retry(upload_target=upload_target, progress=progress,
                                                             task_id=task_id, update_progress=update_progress,
                                                             file_path=file_path)
        update_progress('Finished uploading file', completed=2)

        # step 3 - create manifest file and upload it
        update_progress("Creating manifest")
        self.__create_manifest_for_video(file_path=file_path, manifest=manifest)
        await self.__multipart_upload_single_file(upload_target=upload_target, progress=progress, task_id=task_id,
                                                  update_progress=update_progress, file_path=manifest)
        update_progress('Finished creating manifest', completed=3)

        # step 4 - finish the upload
        update_progress("Finishing upload")
        await self.__finish_upload(session_upload=session_upload, session=session, update_progress=update_progress)
        update_progress('Finished upload', completed=4)

        # step 5 - monitor the progress of processing
        update_progress("Monitoring Panopto processing")
        await self.__monitor_progress(upload_id=upload_id, session=session, update_progress=update_progress,
                                      max_time=MAX_PROCESSING_POLL_TIME)
        update_progress('Finished monitoring', completed=5)

        # step 6 - clean up manifest
        os.unlink(manifest)
        update_progress('Done with file', completed=6)

    async def find_folder(self, session, search_query):
        try:
            url = f'https://{self.server}/Panopto/api/v1/folders/search?searchQuery={search_query}'
            resp = await session.get(url, ssl=self.ssl_verify)
            return await resp.json()
        except aiohttp.ClientResponseError as e:
            # Handle client response errors (e.g., 404, 403, 500)
            print(f'HTTP Error: {e}')
        except aiohttp.ClientError as e:
            # Handle broader aiohttp client errors
            print(f'Aiohttp Error: {e}')
        except Exception as e:
            # Handle other errors (e.g., from JSON parsing)
            print(f'Unexpected Error: {e}')

    async def __create_session(self, session, folder_id, update_progress):
        """
        Create an upload session. Return sessionUpload object.
        """
        while True:
            await asyncio.sleep(DELAY)
            url = f'https://{self.server}/Panopto/PublicAPI/REST/sessionUpload'
            payload = {'FolderId': folder_id}
            headers = {'content-type': 'application/json'}
            resp = await session.post(url=url, json=payload, ssl=self.ssl_verify, headers=headers)
            if not await self.__inspect_response_is_retry_needed(session=session, response=resp,
                                                                 update_progress=update_progress):
                # print('Refreshing token')
                break
        return await resp.json()

    async def __multipart_upload_single_file_with_retry(self, upload_target, file_path, task_id, progress,
                                                        update_progress=None):

        retry_count = 0
        max_retries = 3
        base_delay = 1  # Base delay in seconds

        while True:
            try:
                # Attempt the operation
                await self.__multipart_upload_single_file(upload_target=upload_target, task_id=task_id,
                                                          progress=progress, update_progress=update_progress,
                                                          file_path=file_path)
                break  # If successful, exit the loop

            except Exception as e:
                # Check if it's a retryable error
                if retry_count >= max_retries:
                    # If max retries exceeded, raise the error
                    update_progress(f"[bold][red]Retry limit of {max_retries} reached[/red][/bold]")
                    raise e
                else:
                    # Increment retry count
                    retry_count += 1

                    # Calculate exponential backoff with jitter
                    delay = base_delay * 2 ** retry_count + random.uniform(0, 1)

                    # Log the retry attempt
                    update_progress(f"[bold][yellow]Retry attempt {retry_count} after {delay} seconds[/yellow][/bold]")

                    # Wait for the calculated delay before retrying
                    await asyncio.sleep(delay)

    async def __multipart_upload_single_file(self, upload_target, file_path, task_id, progress, update_progress=None):

        # Upload target which is returned by sessionUpload API consists of:
        # https://{service endpoint}/{bucket}/{prefix}
        # where {bucket} and {prefix} are single element (without delimiter) individually.
        workers = 20
        elements = upload_target.split('/')
        endpoint_url = '/'.join(elements[:-2])
        bucket = elements[-2]
        prefix = elements[-1]
        object_key = f'{prefix}/{os.path.basename(file_path)}'

        session = aioboto3.Session()

        botocore_config = Config(max_pool_connections=workers)

        async with session.client("s3",
                                  endpoint_url=endpoint_url,
                                  verify=self.ssl_verify,
                                  use_ssl=True,
                                  aws_access_key_id="wow",
                                  aws_secret_access_key="wow",
                                  config=botocore_config) as s3:

            with open(file_path, 'rb') as file:

                file_size = os.path.getsize(file_path)
                file_size_mb = bytes_to_megabytes(file_size)

                # Create a new task to monitor upload progress
                upload_progress_task = progress.add_task(
                    f'[green][dim]Uploading[/dim] {os.path.basename(file_path)}[/green]', total=file_size,
                    visible=True)

                try:
                    def progress_cb(uploaded_bytes):
                        print(f'Uploaded {uploaded_bytes}b')
                        progress.advance(upload_progress_task, advance=uploaded_bytes)

                    transfer_config = S3TransferConfig(multipart_chunksize=PART_SIZE)
                    start_time = time.perf_counter()

                    await s3.upload_fileobj(file, Bucket=bucket, Key=object_key, Callback=progress_cb,
                                            Config=transfer_config)

                    end_time = time.perf_counter()
                    upload_time = end_time - start_time
                    speed_mbps = (file_size / upload_time) / (1024 * 1024)  # Upload speed in MBps

                    # Update the main progress bar
                    msg = f'Uploaded [yellow]{os.path.basename(file_path)}[/yellow] ([green]{file_size_mb}Mb[/green]) in [blue]{upload_time: .2f}s[/blue] with an average speed of [orange]{speed_mbps: .2f}MBps[/orange]'
                    update_progress(msg)

                    # Update the upload progress bar
                    progress.update(upload_progress_task, completed=True, visible=False)

                    return

                except Exception as e:
                    print(e)
                    update_progress(f'[bold][red]{str(e)}[/bold][/red]')

    @staticmethod
    def __create_manifest_for_video(file_path, manifest):
        """
        Create manifest XML file for a single video file, based on template.
        """

        file_name = os.path.basename(file_path)

        print(f'Filename is {file_name}')

        with open('src/upload_manifest_template.xml') as fr:
            template = fr.read()
        content = template \
            .replace('{Title}', file_name) \
            .replace('{Description}', 'This is a video session with the uploaded video file {0}'.format(file_name)) \
            .replace('{Filename}', file_name) \
            .replace('{Date}', datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%f-00:00'))
        with codecs.open(manifest, 'w', 'utf-8') as fw:
            fw.write(content)

    async def __finish_upload(self, session, session_upload, update_progress):
        """
        Finish upload.
        """
        upload_id = session_upload['ID']
        upload_target = session_upload['UploadTarget']

        while True:
            # print('Calling PUT PublicAPI/REST/sessionUpload/{0} endpoint'.format(upload_id))
            await asyncio.sleep(DELAY)
            url = f'https://{self.server}/Panopto/PublicAPI/REST/sessionUpload/{upload_id}'
            payload = copy.copy(session_upload)
            payload['State'] = 1  # Upload Completed
            resp = await session.put(url=url, json=payload)
            if not await self.__inspect_response_is_retry_needed(response=resp, session=session,
                                                                 update_progress=update_progress):
                break

    async def __monitor_progress(self, session, upload_id, max_time, update_progress):
        """
        Polling status API until process completes.
        """

        start_time = time.time()

        async def poll():

            while True:

                # Check if max_time has been exceeded
                if time.time() - start_time >= max_time:
                    update_progress("[red]Max polling time reached. Exiting...")
                    return

                await asyncio.sleep(DELAY)

                url = f'https://{self.server}/Panopto/PublicAPI/REST/sessionUpload/{upload_id}'
                resp = await session.get(url=url)

                if await self.__inspect_response_is_retry_needed(response=resp, session=session,
                                                                 update_progress=update_progress):
                    # If we get Unauthorized and token is refreshed, ignore the response at this time and wait for next
                    # time.
                    continue

                session_upload = await resp.json()
                # See https://support.panopto.com/s/article/Upload-API for state property defs
                state = ['Uploading', 'UploadComplete', 'UploadCancelled', 'Processing', 'Complete', 'ProcessingError',
                         'DeletingFiles', 'Deleted', 'DeletingError']
                key = session_upload['State']
                session_state = state[key] if key < len(state) else 'Unknown'

                update_progress('[dim]State: {0} [blue]Elapsed: {1}s'.format(
                    session_state,
                    round(time.time() - start_time, 2)))

                if session_upload['State'] == 4:  # Complete
                    update_progress(
                        f'[green]State: Finished in [blue][bold]{round(time.time() - start_time, 2)}[/bold][/blue]s')
                    break

        try:
            # Enforce the max_time for the polling operation
            await asyncio.wait_for(poll(), timeout=max_time)

        except asyncio.TimeoutError:
            update_progress(
                f"[bold][yellow]Polling operation timed out. This could be due to the size or format of the file. Panopto will still process the file.[/yellow][/bold]")
