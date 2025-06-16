
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

