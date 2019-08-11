from setuptools import setup, find_packages
version = '0.0.22'

setup(
    name='dbsa',
    version=version,
    description="Database schema definitions",
    packages=find_packages(exclude=['ez_setup']),
    include_package_data=True,
    zip_safe=False,
    author='Bence Faludi',
    author_email='bence@subninja.org',
    license='MIT',
    install_requires=[
        'jinja2',
    ],
    entry_points={
        'console_scripts': [
            'dbsa-markdown = dbsa.markdown:main',
        ],
    },
    test_suite="dbsa.tests",
    url='https://github.com/bfaludi/dbsa',
)
