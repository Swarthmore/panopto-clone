import os


def has_files(directory):
    """
    Check if there are any files in directory.
    """
    for root, _, files in os.walk(directory):
        for file in files:
            return True
    return False
