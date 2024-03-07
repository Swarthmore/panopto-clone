# panopto-clone

Clone a source directory into a Panopto folder

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

## Notes

To reset, delete `.files.cache` and `.folders.cache`. This will force the script to re-create the directory tree in Panopto.

