from setuptools import setup, find_packages
import os

__author__ = "Johannes Hörmann"
__copyright__ = "Copyright 2020, IMTEK Simulation, University of Freiburg"
__maintainer__ = "Johannes Hörmann"
__email__ = "johannes.hoermann@imtek.uni-freiburg.de"
__date__ = "Mar 18, 2020"

module_dir = os.path.dirname(os.path.abspath(__file__))


scm_version_options = {
    'write_to': 'imteksimfw/version.py'
    }


def local_scheme(version):
    """Skip the local version (eg. +xyz of 0.6.1.dev4+gdf99fe2)
    to be able to upload to Test PyPI"""
    return ""


url = 'https://github.com/IMTEK-Simulation/imteksimfw'

if __name__ == "__main__":
    setup(
        author='Johannes Hörmann',
        author_email='johannes.hoermann@imtek.uni-freiburg.de',
        name='imteksimfw',
        description='Fireworks additions',
        long_description=open(os.path.join(module_dir, 'README.md')).read(),
        url=url,
        use_scm_version={
            "root": '.',
            "relative_to": __file__,
            "write_to": os.path.join("imteksimfw", "version.py"),
            "local_scheme": local_scheme},
        license='MIT',
        packages=find_packages(),
        include_package_data=True,
        python_requires='>=3.6.5',
        zip_safe=False,
        install_requires=[
            # fwrlm
            'ansible>=2.9.1',
            'dtoolcore>=3.17.0',
            'dtool-create>=0.23.0',
            'fireworks>=1.9.5',
            'jinja2>=2.10',
            'jinja2-time>=0.2.0',
            'monty>=4.0.2',
            'paramiko>=2.4.2',
            'python-daemon>=2.2.4',
            'pid>=3.0.0',
            'psutil>=5.6.1',
            'six>=1.15.0',
            'ruamel.yaml>=0.16.12',
            'tabulate>=0.8.2',
        ],
        setup_requires=['setuptools_scm'],
        extras_require={
            'testing': [
                # ssh tests
                'mock-ssh-server>=0.8.1',
                # dtool smb tasks tests
                'reuests>=2.24.0',
                'urllib3>=1.25.11',
            ],
        },
        entry_points={
            'console_scripts': [
                'fwrlm = imteksimfw.fireworks.scripts.fwrlm_run:main',
                'render = imteksimfw.fireworks.scripts.render_run:main',
            ]
        },
    )
