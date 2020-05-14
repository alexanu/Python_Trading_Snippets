from setuptools import setup, find_packages

setup(
    name='alpha_vantage',
    version='0.2.0',
    author='PoolTrade',
    author_email='pooltrade_2020@labeip.epitech.eu',
    license='MIT',
    description='Python module to get stock data from the AlphaVantage Api',
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'Topic :: Office/Business :: Financial :: Investment',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3.6',
    ],
    url='https://github.com/PoolTrade/API',
    install_requires=[
        'requests',
        'simplejson'
    ],
    extras_requires={
        'pandas': ['pandas'],
    },
    keywords=['stocks', 'market', 'finance', 'alpha_vantage', 'quotes', 'shares'],
    packages=find_packages(exclude=['test_alpha_vantage']),
    package_data={
        'alpha_vantage': [],
    }
)
