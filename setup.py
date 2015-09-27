from setuptools import setup, find_packages

setup(
    name='hq-framework',
    version='2.0.0.dev1',
    url='https://github.com/herqles-io/hq-framework',
    include_package_data=True,
    license='MIT',
    author='CoverMyMeds',
    description='Herqles Framework',
    package_dir={'': 'src'},
    packages=find_packages('src'),
    install_requires=[
        'hq-lib==2.0.0.dev1',
        'pika==0.9.14',
        'cherrypy==3.8.0',
        'schematics==1.1.0'
    ],
    scripts=['bin/hq-framework']
)
