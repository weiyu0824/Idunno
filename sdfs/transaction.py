class TrasanctionType:
    Put = 1
    Get = 2
    Delete = 3
    List = 4
    Version = 5

# TODO: update to snake's namings
class TransactionParam:
    @staticmethod
    def genPutTxnParams(sdfs_filename: str) -> tuple:
        return (TrasanctionType.Put, sdfs_filename) 

    @staticmethod
    def genGetTxnParams(sdfs_filename: str) -> tuple:
        return (TrasanctionType.Get, sdfs_filename)

    @staticmethod
    def genDeleteTxnParams(sdfs_filename: str) -> tuple:
        return (TrasanctionType.Delete, sdfs_filename)

    @staticmethod
    def genListTxnParams(sdfs_filename: str) -> tuple:
        return (TrasanctionType.List, sdfs_filename)

    @staticmethod
    def genVersionParams(sdfs_filename: str, local_filename: str, num_version: str) -> tuple:
        return (TrasanctionType.Version, sdfs_filename, local_filename, num_version)

class PutTransaction:
    def __init__(self, params: tuple):
        self.txn_type = params[0]
        self.sdfs_filename = params[1]

class GetTransaction:
    def __init__(self, params: tuple):
        self.txn_type = params[0]
        self.sdfs_filename = params[1]

class DeleteTransaction:
    def __init__(self, params: tuple):
        self.txn_type = params[0]
        self.sdfs_filename = params[1]

class ListTransaction:
    def __init__(self, params: tuple):
        self.txn_type = params[0]
        self.sdfs_filename = params[1]

class VersionTransaction:
    def __init__(self, params: tuple):
        self.txn_type = params[0]
        self.sdfs_filename = params[1]
        self.local_filename = params[2]
        self.num_version = params[2]
