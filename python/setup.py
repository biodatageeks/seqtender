from setuptools import setup

setup(
    name='seqtender.py',
    version='0.1.0',
    packages=['seqtender'],
    install_requires=[
        'typeguard==2.5.0',
        'pyspark==2.4.3',
        'findspark'
    ],
    author='biodatageeks.org',
    description='Large-scale genomic pipelines using Spark',
    long_description=open('README.rst').read(),
    long_description_content_type='text/x-rst',
    license='Apache License 2.0',
    classifiers=[
        'Intended Audience :: Developers',
        'Programming Language :: Python :: 3.6',
    ],
    url='https://github.com/ZSI-Bio/bdg-seqtender'
)