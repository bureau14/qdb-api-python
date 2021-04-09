
PYTHON="${PYTHON_CMD:-python}"

rm -r -fo dist/
${PYTHON} setup.py bdist_egg -d dist/
${PYTHON} setup.py bdist_wheel -d dist/