.PHONY: pivot

all: env deps test

env:
	virtualenv env
	./env/bin/pip install pytest ipython

deps:
	./env/bin/pip install -r requirements.txt

test:
	./env/bin/py.test -x ./pivot/

shell:
	./env/bin/ipython --config=shell.py
