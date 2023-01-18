class FileMessageType:
    DeleteOne = 1
    DeleteAll = 2
    PUT = 3

class FileMessage:
    def __init__(self, msgType: int, filename: str, version:int=None):
        self.msg_type = msgType
        self.filename = filename
        self.version = version