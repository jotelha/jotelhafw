from setuptools import setup, find_packages
import os
import versioneer

__author__     = "Johannes Hörmann"
__copyright__  = "Copyright 2019, IMTEK Simulation, University of Freiburg"
__maintainer__ = "Johannes Hörmann"
__email__      = "johannes.hoermann@imtek.uni-freiburg.de"
__date__       = "Mar 18, 2020"

module_dir = os.path.dirname(os.path.abspath(__file__))

if __name__ == "__main__":
    setup(
        name='imteksimfw',
        version=versioneer.get_version(),
        cmdclass=versioneer.get_cmdclass(),
        description='Fireworks additions',
        long_description=open(os.path.join(module_dir, 'README.md')).read(),
        url='https://github.com/IMTEK-Simulation/imteksimfw',
        author='Johannes Hörmann',
        author_email='johannes.hoermann@imtek.uni-freiburg.de',
        license='MIT',
        packages=find_packages(),
        package_data={'': ['ChangeLog.md']},
        python_requires='>=3.6.5',
        zip_safe=False,
        install_requires=[
            'fireworks>=1.9.5' ]
    )
