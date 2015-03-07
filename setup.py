try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

config = {
    'description': 'Synced: ES & Cassandra as one',
    'author': 'Roberto Weidmann Menezes',
    'url': 'URL to get it at.',
    'download_url': 'Where to download it.',
    'author_email': 'robertowm (at) gmail (dot) com',
    'version': '0.1',
    'install_requires': ['nose', 'pyes', 'cassandra-driver'],
    'packages': ['synced'],
    'scripts': [],
    'name': 'synced'
}

setup(**config)