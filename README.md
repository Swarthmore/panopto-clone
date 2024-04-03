# panopto-clone

Clone a source directory into a Panopto folder

## Dependencies

* `python3-venv`

## Quickstart

```bash
# Create a python3 virtual environment (requires python3-venv package)
python 3 -m venv .

# Enter your new environment
source bin/activate

# Install python dependencies
pip install -r requirements.txt

# Run python_clone script (you may need to chmod +X it
./panopto_clone --help
```

## How to Use

```
usage: panopto_clone.py [-h] --server SERVER --destination DESTINATION --source SOURCE --client-id CLIENT_ID --client-secret CLIENT_SECRET
                        [--skip-verify] [--manifest-template MANIFEST_TEMPLATE] [--clean] [--max-concurrent-tasks MAX_CONCURRENT_TASKS]

Upload a folder to Panopto

options:
  -h, --help            show this help message and exit
  --server SERVER       Server name as FQDN
  --destination DESTINATION
                        ID of target Panopto folder
  --source SOURCE       Absolute path to source folder
  --client-id CLIENT_ID
                        Client ID of OAuth2 client
  --client-secret CLIENT_SECRET
                        Client Secret of OAuth2 client
  --skip-verify         (optional) Skip SSL certificate verification. (Never apply to the production code)
  --manifest-template MANIFEST_TEMPLATE
                        (optional, default=src/upload_manifest_template.xml) Absolute path to manifest template
  --clean               (optional) Force removal of .cache files. WARNING: Doing this will likely create duplicate folders.
  --max-concurrent-tasks MAX_CONCURRENT_TASKS
                        (optional, default=5) How many uploads should occur concurrently.
```

## Issues & Contributions

Are welcome!
