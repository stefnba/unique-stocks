from pathlib import Path

from utils.filesystem.path import LocalPath, PathInput


def read_large_file(file_path: PathInput, chunk_size=4 * 1024 * 1024):
    """Generator function to read a large file in chunks."""

    with open(LocalPath.create(file_path).uri, "rb") as file:
        while True:
            data = file.read(chunk_size)
            if not data:
                break
            yield data


class StreamDiskFile:
    """
    Read file from disk in chunks.
    """

    path: str

    def __init__(self, path: str) -> None:
        self.path = path

    def get_file_size(self):
        """Return file size in bytes."""
        return Path(self.path).stat().st_size

    def iter_content(self, chunk_size=1024 * 1024):
        """Iterate over file in chunks."""
        self.get_file_size()
        self.get_file_size() / 1024 / 1024

        # logger.file.info(
        #     "Initiating file read with stream.",
        #     event=logger_events.file.StreamInit(sizeBytes=size, sizeMegaBytes=sizeMB),
        # )

        def generator():
            with open(self.path, "rb") as f:
                first_iteration = True

                while content := f.read(chunk_size):
                    # log first read iteration
                    if first_iteration:
                        # logger.file.info(
                        #     "Start reading file with stream.",
                        #     event=logger_events.file.StreamStart(sizeBytes=size, sizeMegaBytes=sizeMB),
                        # )
                        first_iteration = False
                    # log last read iteration
                    # if f.tell() == self.get_file_size():
                    # logger.file.info(
                    #     "Finished file read with stream.",
                    #     event=logger_events.file.StreamSuccess(sizeBytes=size, sizeMegaBytes=sizeMB),
                    # )

                    yield content

        return generator()
