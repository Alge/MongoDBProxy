"""
Copyright 2013 Gustav Arngarden
Copyright 2025 Martin Alge <martin@alge.se>

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
"""

from setuptools import setup, find_packages

with open('README.rst') as readme_file:
    LONG_DESCRIPTION = readme_file.read()

setup(
    name='MongoDBProxy-official',
    packages=find_packages(),
    version='0.2.0',
    description='Proxy around MongoDB connection that automatically handles AutoReconnect exceptions.',
    author='Martin Alge',
    author_email='martin@alge.se',
    maintainer='Martin Alge',
    maintainer_email='martin@alge.se',
    long_description=LONG_DESCRIPTION,
    classifiers=[
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
    ],
    python_requires='>=3.6',
    install_requires=['pymongo>=4.0,<5.0'],
    test_suite='tests',
    url="https://github.com/Alge/MongoDBProxy.git",
)
