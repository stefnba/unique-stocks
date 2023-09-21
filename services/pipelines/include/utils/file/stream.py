from pathlib import Path


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
        size = self.get_file_size()
        sizeMB = self.get_file_size() / 1024 / 1024

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
