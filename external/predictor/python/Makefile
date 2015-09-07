SHEL=/bin/bash


bdist:
	python setup.py bdist_wheel --universal

sdist:
	python setup.py sdist

dist:sdist bdist

push:
	twine upload dist/*
