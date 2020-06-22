class RelativePathError(Exception):
    pass


class CollectionConfigParsingError(Exception):
    pass


class CollectionConfigFileNotFoundError(Exception):
    pass


class CollectionError(Exception):
    def __init__(self, collection=None, message=None):
        super().__init__(message)
        self.collection = collection


class MissingValueCollectionError(CollectionError):
    def __init__(self, missing_value, collection=None, message=None):
        super().__init__(collection, message)
        self.missing_value = missing_value


class ConflictingPathCollectionError(CollectionError):
    pass


class RelativePathCollectionError(CollectionError):
    pass
