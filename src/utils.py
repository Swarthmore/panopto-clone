import os
import pickle


def save_dict_to_disk(data_dict, file_path):
    """
    Saves a dictionary to disk using pickle.

    :param data_dict: Dictionary to be saved.
    :param file_path: Path to the file where the dictionary will be saved.
    """
    with open(file_path, 'wb') as file:
        pickle.dump(data_dict, file)


def retrieve_dict_from_disk(file_path):
    """
    Retrieves a dictionary from disk using pickle.

    :param file_path: Path to the file from which the dictionary will be retrieved.
    :return: The dictionary retrieved from the file.
    """
    with open(file_path, 'rb') as file:
        data_dict = pickle.load(file)
    return data_dict


def has_files(directory):
    """
    Check if there are any files in directory.
    """
    for root, _, files in os.walk(directory):
        for _ in files:
            return True
    return False


def write_list_to_file(file_name, list_variable):
    with open(file_name, 'w') as file:
        # Ensure each item ends with a newline character
        lines = [f"{item}\n" for item in list_variable]
        file.writelines(lines)
