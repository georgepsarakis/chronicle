import os
from setuptools import setup, find_packages


requirements = dict.fromkeys(('dev', 'test', 'base'))

for section in requirements:
    file_path = os.path.join(
        'install',
        'requirements',
        '{}.txt'.format(section)
    )
    if os.path.exists(file_path):
        with open(file_path, 'r') as f:
            requirements[section] = list(f.readlines())

setup(
    name='chronicle',
    version='0.0.1',
    package_dir={'': '.'},
    packages=find_packages(),
    zip_safe=False,
    install_requires=requirements['base'],
    extras_require={
        'redis': [
            'redis==3.3.8'
        ],
        'dev': requirements.get('dev', [])
    },
    include_package_data=True,
    classifiers=[
        'Framework :: Django',
        'Intended Audience :: Developers',
        'Intended Audience :: System Administrators',
        'Operating System :: POSIX :: Linux',
        'Development Status :: 2 - Pre-Alpha'
    ],
    entry_points='''
        [console_scripts]
        chronicle=chronicle.cli:cli
    '''
)
