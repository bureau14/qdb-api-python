python3 -m pip install dist/quasardb*.whl
python3 -m pip install -r dev-requirements.txt
mkdir doc || true
python3 docgen.py
tar -czvf dist/doc.tar.gz doc/*
