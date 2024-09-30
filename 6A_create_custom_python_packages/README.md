This is a simple example package. You can use
refer to :" [here](https://packaging.python.org/en/latest/tutorials/packaging-projects/)

Demo Script:
Delete earlier Package version in dist
Change the .toml file for new version

pyenv --help
pyenv virtualenvs
pyenv activate test-cde
pip install build 
pip install twine
python3 -m build

python3 -m twine upload --repository testpypi dist/*
username: __token__
get the pypi token from Notes

SHOW THE Package in Pypi : https://test.pypi.org/project/example-package-superellipse/#history

