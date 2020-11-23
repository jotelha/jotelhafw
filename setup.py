import os

from setuptools import setup, find_packages
from setuptools_scm import get_version

__author__ = "Johannes Hörmann"
__copyright__ = "Copyright 2020, IMTEK Simulation, University of Freiburg"
__maintainer__ = "Johannes Hörmann"
__email__ = "johannes.hoermann@imtek.uni-freiburg.de"
__date__ = "Mar 18, 2020"

module_dir = os.path.dirname(os.path.abspath(__file__))
readme = open(os.path.join(module_dir, 'README.md')).read()
version = get_version(root='.', relative_to=__file__)


def local_scheme(version):
    """Skip the local version (eg. +xyz of 0.6.1.dev4+gdf99fe2)
    to be able to upload to Test PyPI"""
    return ""


url = 'https://github.com/IMTEK-Simulation/imteksimfw'

if __name__ == "__main__":
    setup(
        author='Johannes Laurin Hoermann',
        author_email='johannes.hoermann@imtek.uni-freiburg.de',
        name='imteksimfw',
        description='Fireworks additions',
        long_description=readme,
        long_description_content_type="text/markdown",
        url=url,
        use_scm_version={
            "root": '.',
            "relative_to": __file__,
            "write_to": os.path.join("imteksimfw", "version.py"),
            "local_scheme": local_scheme},
        packages=find_packages(),
        include_package_data=True,
        python_requires='>=3.6.5',
        zip_safe=False,
        install_requires=[
            'dill>=0.3.1.1'
            'dtoolcore>=3.17.0',
            'dtool-create>=0.23.0',
            'dtool-lookup-api>=0.1.0',
            'fireworks>=1.9.5',
            'jinja2>=2.10',
            'jinja2-time>=0.2.0',
            'monty>=4.0.2',
            'paramiko>=2.4.2',
            'six>=1.15.0',
            'ruamel.yaml>=0.16.12',
        ],
        setup_requires=['setuptools_scm', 'pytest-runner'],
        tests_require=['pytest'],
        extras_require={
            'testing': [
                # ssh tests
                'mock-ssh-server>=0.8.1',
                # dtool smb tasks tests
                'dtool-smb>=0.1.0',
                'requests>=2.24.0',
                'urllib3<1.26,>=1.25.11',
                'dtool-lookup-server>=0.15.0',
                'dtool-lookup-server-direct-mongo-plugin>=0.1.2',
            ],
        },
        download_url="{}/tarball/{}".format(url, version),
        license='MIT',
    )
