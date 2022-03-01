#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from setuptools import setup, find_packages
from os import path

with open(path.join(path.dirname(__file__), "README.md"), "r") as README:
    long_description = README.read()

setup(
    name="pet_exchange",
    version="1.0.0",
    author="Jacob Wahlman",
    description="Privacy-Enhanced Trading Exchange (PET-Exchange)",
    long_description=long_description,
    long_description_content_type="text/markdown",
    packages=find_packages(),
    python_requires=">=3.9",
    install_requires=[
        "grpcio>=1.43.0",
        "pyfhel==2.3.1",
        "grpcio-tools>=1.43.0",
        "matplotlib>=3.5.1",
        "numpy>=1.22.2",
    ],
    extras_require={"dev": ["black>=21.7b0"]},
)
