import os
from utils import has_files


async def create_directory_skeleton(source_directory, uploader, session, created_folders=None, parent_folder_id=None):
    """
    Create folders in Panopto that match the local tree (empty folders are not created)
    """
    if created_folders is None:
        created_folders = {}

    for item in os.listdir(source_directory):

        item_path = os.path.join(source_directory, item)

        if os.path.isdir(item_path):

            # Only process if there are files in item_path
            # if has_files(item_path):
            # Create the folder

            fp = os.path.splitext(os.path.basename(item_path))[0]
            # print(f'Creating folder {fp}')
            folder = await uploader.create_folder(
                folder_id=parent_folder_id,
                folder_name=os.path.basename(item_path),
                folder_description="Created by panopto_clone.py",
                session=session)
            created_folders[fp] = folder

            # Recurse into the directory after creating it in Panopto
            await create_directory_skeleton(
                source_directory=item_path,
                uploader=uploader,
                session=session,
                parent_folder_id=folder['Id'],
                created_folders=created_folders)

    return created_folders
