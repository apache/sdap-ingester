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


package.zip: lambda_function.py ../requirements.txt
	rm -f package.zip
	rm -rf package/
	mkdir package
	pip install --target ./package -r ../requirements.txt
	cd package && zip -9r ../package.zip .
	zip -9 package.zip lambda_function.py
	rm -rf package/


package_maap.zip: lambda_function.py ../requirements.txt
	rm -f package_maap.zip
	rm -rf package_maap/
	mkdir package_maap
	pip install \
		--target ./package_maap \
		--platform manylinux2014_x86_64 \
		--implementation cp \
		-r ../requirements_maap.txt \
		--python-version 3.9 \
		--only-binary=:all: --upgrade
	cd package_maap && zip -9r ../package_maap.zip .
	zip -9 package_maap.zip lambda_function.py
	rm -rf package_maap/

