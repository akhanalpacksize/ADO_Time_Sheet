import os


def create_folder_if_does_not_exist(dir_name):
    if not os.path.exists(dir_name):
        os.makedirs(dir_name)