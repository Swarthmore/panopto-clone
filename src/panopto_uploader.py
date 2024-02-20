#!python3
import os
import aiohttp
import codecs
import time
from datetime import datetime
import copy
import aioboto3

# Size of each part of multipart upload.
# This must be between 5MB and 25MB. Panopto server may fail if the size is more than 25MB.
PART_SIZE = 5 * 1024 * 1024

# Template for manifest XML file.
MANIFEST_FILE_TEMPLATE = 'src/upload_manifest_template.xml'

# Filename of manifest XML file. Any filename is acceptable.
MANIFEST_FILE_NAME = 'upload_manifest_generated.xml'


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
            print('Forbidden. This may mean token expired. Refreshing access token.')
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
                text = await res.text()
                return

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

    async def upload_video(self, session, file_path, folder_id):
        """
        Main upload method to go through all required steps.
        """
        # step 1 - Create a session
        session_upload = await self.__create_session(folder_id, session)
        upload_id = session_upload['ID']
        upload_target = session_upload['UploadTarget']

        # step 2 - upload the video file
        await self.__multipart_upload_single_file(upload_target, file_path)

        # step 3 - create manifest file and upload it
        self.__create_manifest_for_video(file_path, MANIFEST_FILE_NAME)
        await self.__multipart_upload_single_file(upload_target, MANIFEST_FILE_NAME)

        # step 4 - finish the upload
        await self.__finish_upload(session_upload, session)

        # step 5 - monitor the progress of processing
        await self.__monitor_progress(upload_id, session)

    async def __create_session(self, session, folder_id):
        """
        Create an upload session. Return sessionUpload object.
        """
        while True:
            url = f'https://{self.server}/Panopto/PublicAPI/REST/sessionUpload'
            payload = {'FolderId': folder_id}
            resp = await session.post(url=url, json=payload)
            if not self.__inspect_response_is_retry_needed(session, response=resp):
                break
        return await resp.json()

    async def __multipart_upload_single_file(self, upload_target, file_path):

        # Upload target which is returned by sessionUpload API consists of:
        # https://{service endpoint}/{bucket}/{prefix}
        # where {bucket} and {prefix} are single element (without delimiter) individually.
        elements = upload_target.split('/')
        endpoint_url = '/'.join(elements[:-2])
        bucket = elements[-2]
        prefix = elements[-1]
        object_key = f'{prefix}/{os.path.basename(file_path)}'

        session = aioboto3.Session()
        async with session.resource("s3", endpoint_url, verify=self.ssl_verify) as s3:
            mpu = await s3.create_multipart_upload(Bucket=bucket, Key=object_key)
            parts = []
            part_number = 1

            # Read and upload each part
            with open(file_path, 'rb') as file:
                while True:
                    data = file.read(PART_SIZE)
                    if not data:
                        break

                    part = await s3.upload_part(
                        Bucket=bucket,
                        Key=object_key,
                        PartNumber=part_number,
                        UploadId=mpu['UploadId'],
                        Body=data
                    )

                    parts.append({
                        'PartNumber': part_number,
                        'ETag': part['ETag']
                    })
                    part_number += 1

            # Complete the upload
            await s3.complete_multipart_upload(
                Bucket=bucket,
                Key=object_key,
                UploadId=mpu['UploadId'],
                MultipartUpload={'Parts': parts}
            )

            print(f'Upload of {file_path} to {object_key} complete.')

    @staticmethod
    def __create_manifest_for_video(file_path, manifest_file_name):
        """
        Create manifest XML file for a single video file, based on template.
        """
        # print('Writing manifest file: {0}'.format(manifest_file_name))

        file_name = os.path.basename(file_path)

        with open(MANIFEST_FILE_TEMPLATE) as fr:
            template = fr.read()
        content = template \
            .replace('{Title}', file_name) \
            .replace('{Description}', 'This is a video session with the uploaded video file {0}'.format(file_name)) \
            .replace('{Filename}', file_name) \
            .replace('{Date}', datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%f-00:00'))
        with codecs.open(manifest_file_name, 'w', 'utf-8') as fw:
            fw.write(content)

    async def __finish_upload(self, session, session_upload):
        """
        Finish upload.
        """
        upload_id = session_upload['ID']
        upload_target = session_upload['UploadTarget']

        while True:
            print('Calling PUT PublicAPI/REST/sessionUpload/{0} endpoint'.format(upload_id))
            url = f'https://{self.server}/Panopto/PublicAPI/REST/sessionUpload/{upload_id}'
            payload = copy.copy(session_upload)
            payload['State'] = 1  # Upload Completed
            resp = await session.put(url=url, json=payload)
            if not await self.__inspect_response_is_retry_needed(resp):
                break

    async def __monitor_progress(self, session, upload_id):
        """
        Polling status API until process completes.
        """
        while True:
            time.sleep(5)
            url = f'https://{self.server}/Panopto/PublicAPI/REST/sessionUpload/{upload_id}'
            resp = await session.get(url=url)
            if self.__inspect_response_is_retry_needed(resp):
                # If we get Unauthorized and token is refreshed, ignore the response at this time and wait for next
                # time.
                continue
            session_upload = await resp.json()
            print('  State: {0}'.format(session_upload['State']))

            if session_upload['State'] == 4:  # Complete
                break
