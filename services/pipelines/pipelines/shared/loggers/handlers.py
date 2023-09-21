from shared.utils.logging.handlers import FileHandler, ConsoleHandler, HttpHandler

console_handler = ConsoleHandler()
file_handler = FileHandler(path="./logs/")
http_handler = HttpHandler()
