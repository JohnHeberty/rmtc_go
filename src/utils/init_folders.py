from os.path import exists, join, dirname
from os import makedirs

def init_folders(folders: list[str]) -> None:
    """
    Initialize folders in the specified base path.

    Args:
        folders (list[str]): A list of folder naMONTH to be created.

    Raises:
        OSError: If there is an error creating a folder.
    """
    for folder in folders:
        folder_path = folder
        if not exists(folder_path):
            try:
                makedirs(folder_path)
                base_folder = dirname(folder_path)
                open(join(base_folder, ".METADATA"), 'a').close()  # Create an empty .METADATA file
                print(f"Folder created: {folder_path}")
            except OSError as e:
                print(f"Error creating folder {folder_path}: {e}")
