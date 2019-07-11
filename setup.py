from setuptools import setup, find_packages

setup(
    name='cb-stackcollector',
    version='0.4',
    packages=find_packages(),
    include_package_data=True,
    install_requires=[
        'requests>=2.4.3',
        'flask>=0.10.1',
        'click',
        'dateparser'
    ],
)
