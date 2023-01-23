# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

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
