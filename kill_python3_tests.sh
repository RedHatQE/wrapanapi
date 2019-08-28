#!/bin/bash

BLACKLISTED_TEST_FILES=test_redfish.py

PYTHON_VERSION=$(python -c 'import sys ; print("%d%d"%sys.version_info[0:2])')

echo "Python at version $PYTHON_VERSION"
if (( "$PYTHON_VERSION" < "36" ))
then
	echo "Blacklisting files that require at least Python 3.6"
	for F in $BLACKLISTED_TEST_FILES
	do
		echo "tests/$F"
		[[ -f "tests/$F" ]] && mv "tests/$F" "tests/$F.disable"
	done
fi
