# panopto-clone

Clone a source directory into a Panopto folder


## Scripts

`python3 src/panopto_upload_file.py` - Upload a single file to Panopto

```
usage: panopto_upload_file.py [-h] --server SERVER --folder-id FOLDER_ID --upload-file UPLOAD_FILE --client-id CLIENT_ID --client-secret CLIENT_SECRET [--skip-verify]

Upload a single video file to Panopto server

options:
  -h, --help            show this help message and exit
  --server SERVER       Server name as FQDN
  --folder-id FOLDER_ID
                        ID of target Panopto folder
  --upload-file UPLOAD_FILE
                        File to be uploaded
  --client-id CLIENT_ID
                        Client ID of OAuth2 client
  --client-secret CLIENT_SECRET
                        Client Secret of OAuth2 client
  --skip-verify         Skip SSL certificate verification. (Never apply to the production code)
```

`python3 src/panopto_create_folder.py` - Create a folder in Panopto

```
Not implemented
```

`python3 src/panopto_create_folder_tree.py` - Create a folder tree in Panopto

```
Not implemented
```

`python3 src/panopto_clone.py` - Clone a source directory into Panopto