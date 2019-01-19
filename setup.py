from setuptools import setup


with open("README.md", 'r') as f:
    long_description = f.read()


setup(
    name='WrappyDatabase',
    version='1.0.3',
    packages=['WrappyDatabase'],
    url='https://github.com/geekmoss/WrappyDatabase',
    license='MIT',
    author='Jakub Janeček',
    author_email='Jakub.Janecek@firma.seznam.cz',
    description='Python module with universal interface for PostgreSQL and MySQL.',
    long_description=long_description,
    long_description_content_type="text/markdown",
    install_requires=['psycopg2-binary', 'pymysql']
)
