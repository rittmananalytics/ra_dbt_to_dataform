from setuptools import setup, find_packages

setup(
    name='dbt-to-dataform',
    version='0.1.0',
    packages=find_packages(),
    include_package_data=True,
    install_requires=[
        'PyYAML==6.0',
        'langchain==0.0.252',
        'openai==0.28.1',
        'pathlib==1.0.1',
        'typing-extensions==4.7.1',
        'pytest==7.4.0'
    ],
    entry_points={
        'console_scripts': [
            'dbt-to-dataform=dbt_to_dataform.main:cli',
        ],
    },
)