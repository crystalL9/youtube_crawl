from setuptools import setup
from Cython.Build import cythonize
import os
import glob

# Lấy danh sách các file .py trong thư mục hiện tại và các thư mục con
py_files = glob.glob("**/*.py", recursive=True)

setup(
    ext_modules=cythonize(py_files)
)