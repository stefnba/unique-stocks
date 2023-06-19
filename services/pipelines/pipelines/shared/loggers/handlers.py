from shared.utils.logging import Handlers

console_handler = Handlers.Console()
file_handler = Handlers.File(file_path="./logs/")
http_handler = Handlers.Http()
