import asyncio
import os
import aiohttp
import codecs
import time
from datetime import datetime
import copy
import aioboto3
import math

# Size of each part of multipart upload.
# This must be between 5MB and 25MB. Panopto server may fail if the size is more than 25MB.
PART_SIZE = 25 * 1024 * 1024


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

    async def __inspect_response_is_retry_needed(self, session, response):
        """
        Inspect the response of an aiohttp call.
        True indicates the retry needed, False indicates success. Otherwise, an exception is thrown.

        This method detects 403 (Forbidden), refreshes the access token, and returns as 'is retry needed'.
        """
        if response.status == 200:
            # Success on 2xx response.
            return False

        if response.status == 403:
            # print('Forbidden. This may mean token expired. Refreshing access token.')
            self.__setup_or_refresh_access_token(session)  # Ensure this method is async and awaits the token refresh
            return True

        # For aiohttp, use response.raise_for_status() to automatically throw if the status is an error code
        # Make sure to use it where it doesn't preempt your checks for recoverable error codes like 403
        try:
            response.raise_for_status()
        except aiohttp.ClientError as e:
            # Handle specific aiohttp exceptions or re-raise
            # This is where you might log the error or handle specific HTTP errors differently
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
            payload = {'Name': folder_name, 'Description': folder_description, 'Parent': folder_id}
            res = await session.post(url, json=payload, ssl=self.ssl_verify)

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
            res = await session.get(url, ssl=self.ssl_verify)
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

    async def upload_video_with_progress(self, session, folder_id, file_path, progress, task_id, manifest_file_name, manifest_file_template):
        def update_progress(msg, completed=None):
            progress.console.log(f"[bold][{task_id}][/bold] [dim]{os.path.basename(file_path)}->{folder_id}[/dim] {msg}")  # Log the current step
            progress.update(task_id, visible=True)
            # Optionally, update the task's progress bar here
            if completed:
                progress.update(task_id, completed=completed)

        await self.upload_video(session, file_path, folder_id, progress, task_id, update_progress, manifest_file_name, manifest_file_template)
        progress.update(task_id, completed=100)  # Mark the task as completed

    async def upload_video(self, session, file_path, folder_id, progress, task_id, update_progress, manifest_file_name, manifest_file_template):
        """
        Main upload method to go through all required steps.
        """
        # step 1 - Create a session
        update_progress("Creating session")
        session_upload = await self.__create_session(session=session, folder_id=folder_id)
        upload_id = session_upload['ID']
        upload_target = session_upload['UploadTarget']

        # step 2 - upload the video file
        update_progress("Uploading file")
        await self.__multipart_upload_single_file(upload_target=upload_target, file_path=file_path, progress=progress, task_id=task_id, update_progress=update_progress)

        # step 3 - create manifest file and upload it
        update_progress("Creating manifest")
        self.__create_manifest_for_video(file_path, manifest_file_name, manifest_file_template)
        await self.__multipart_upload_single_file(upload_target, file_path=manifest_file_name,  progress=progress, task_id=task_id, update_progress=update_progress)

        # step 4 - finish the upload
        update_progress("Finishing upload")
        await self.__finish_upload(session_upload=session_upload, session=session, update_progress=update_progress)

        # step 5 - monitor the progress of processing
        update_progress("Monitoring Panopto processing")
        await self.__monitor_progress(upload_id=upload_id, session=session, update_progress=update_progress, max_time=300)

        # step 6 - clean up manifest
        os.unlink(manifest_file_name)

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

    async def __create_session(self, session, folder_id):
        """
        Create an upload session. Return sessionUpload object.
        """
        while True:
            url = f'https://{self.server}/Panopto/PublicAPI/REST/sessionUpload'
            payload = {'FolderId': folder_id}
            headers = {'content-type': 'application/json'}
            resp = await session.post(url=url, json=payload, ssl=self.ssl_verify, headers=headers)
            if not await self.__inspect_response_is_retry_needed(session=session, response=resp):
                # print('Refreshing token')
                break
        return await resp.json()

    async def __multipart_upload_single_file(self, upload_target, file_path, task_id, progress, update_progress=None):

        # Upload target which is returned by sessionUpload API consists of:
        # https://{service endpoint}/{bucket}/{prefix}
        # where {bucket} and {prefix} are single element (without delimiter) individually.
        elements = upload_target.split('/')
        endpoint_url = '/'.join(elements[:-2])
        bucket = elements[-2]
        prefix = elements[-1]
        object_key = f'{prefix}/{os.path.basename(file_path)}'

        session = aioboto3.Session()

        async with session.client("s3", endpoint_url=endpoint_url, verify=self.ssl_verify, use_ssl=True,
                                  aws_access_key_id="wow", aws_secret_access_key="wow") as s3:

            mpu = await s3.create_multipart_upload(Bucket=bucket, Key=object_key)
            parts = []
            part_number = 1
            file_size = os.path.getsize(file_path)
            total_parts = math.ceil(file_size / PART_SIZE)
            uploaded_bytes = 0

            # Read and upload each part
            # print(f'Opening {file_path} {file_size_in_mb} mb')
            with open(file_path, 'rb') as file:

                while True:
                    data = file.read(PART_SIZE)
                    if not data:
                        break

                    start_time = time.perf_counter()
                    part = await s3.upload_part(
                        Bucket=bucket,
                        Key=object_key,
                        PartNumber=part_number,
                        UploadId=mpu['UploadId'],
                        Body=data
                    )
                    end_time = time.perf_counter()

                    parts.append({
                        'PartNumber': part_number,
                        'ETag': part['ETag']
                    })
                    part_number += 1
                    uploaded_bytes += len(data)

                    upload_time = end_time - start_time  # Time taken to upload the part
                    speed_mbps = (len(data) / upload_time) / (1024 * 1024)  # Upload speed in MBps

                    pct_complete = round((uploaded_bytes / file_size) * 100, 2)
                    update_progress(f'[blue]Elapsed: {upload_time:.2f} [yellow]Speed: {speed_mbps:.2f} MBps [red]Uploaded: {uploaded_bytes}b [green]Part: {part_number}/{total_parts}', completed=pct_complete)
                    progress.update(task_id, refresh=True)

            # Complete the upload
            await s3.complete_multipart_upload(
                Bucket=bucket,
                Key=object_key,
                UploadId=mpu['UploadId'],
                MultipartUpload={'Parts': parts}
            )
            update_progress(f'[dark_goldenrod][bold]Upload complete')


    @staticmethod
    def __create_manifest_for_video(file_path, manifest_file_name, manifest_file_template):
        """
        Create manifest XML file for a single video file, based on template.
        """

        file_name = os.path.basename(file_path)

        with open(manifest_file_template) as fr:
            template = fr.read()
        content = template \
            .replace('{Title}', file_name) \
            .replace('{Description}', 'This is a video session with the uploaded video file {0}'.format(file_name)) \
            .replace('{Filename}', file_name) \
            .replace('{Date}', datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%f-00:00'))
        with codecs.open(manifest_file_name, 'w', 'utf-8') as fw:
            fw.write(content)

    async def __finish_upload(self, session, session_upload, update_progress):
        """
        Finish upload.
        """
        upload_id = session_upload['ID']
        upload_target = session_upload['UploadTarget']

        while True:
            # print('Calling PUT PublicAPI/REST/sessionUpload/{0} endpoint'.format(upload_id))
            url = f'https://{self.server}/Panopto/PublicAPI/REST/sessionUpload/{upload_id}'
            payload = copy.copy(session_upload)
            payload['State'] = 1  # Upload Completed
            resp = await session.put(url=url, json=payload)
            if not await self.__inspect_response_is_retry_needed(response=resp, session=session):
                update_progress(f'[bold][green]Finished uploading to {upload_target}')
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

                await asyncio.sleep(5)

                url = f'https://{self.server}/Panopto/PublicAPI/REST/sessionUpload/{upload_id}'
                resp = await session.get(url=url)
                if await self.__inspect_response_is_retry_needed(response=resp, session=session):
                    # If we get Unauthorized and token is refreshed, ignore the response at this time and wait for next
                    # time.
                    continue
                session_upload = await resp.json()
                update_progress('[green]State: {0} [blue]Elapsed: {1}s'.format(session_upload['State'], round(time.time() - start_time, 2)))

                if session_upload['State'] == 4:  # Complete
                    update_progress(f'[green][bold]State: 4 - Finished!')
                    break

        try:
            # Enforce the max_time for the polling operation
            await asyncio.wait_for(poll(), timeout=max_time)
        except asyncio.TimeoutError:
            print("Polling operation timed out.")
