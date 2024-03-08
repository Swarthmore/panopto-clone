CACHE_CREATED_FOLDERS = '.created_folders.cache'
CACHE_FILES_TO_UPLOAD = '.files_to_upload.cache'
CACHE_UPLOADED_FILES = '.uploaded_files.cache'

# Size of each part of multipart upload.
# This must be between 5MB and 25MB. Panopto server may fail if the size is more than 25MB.
PART_SIZE = 8 * 1024 * 1024

# Template for manifest XML file.
MANIFEST_FILE_TEMPLATE = 'src/upload_manifest_template.xml'

# Filename of manifest XML file. Any filename is acceptable.
MANIFEST_FILE_NAME = 'upload_manifest_generated.xml'

# The max amount of time (in seconds) to monitor the processing stage of an upload.
MAX_PROCESSING_POLL_TIME = 60

# Artificial delay in async loop to prevent hitting api rates and other errors
DELAY = 1
