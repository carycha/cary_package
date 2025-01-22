from setuptools import setup, find_packages

setup(
    name="cary_package",
    version="0.0.1",
    author="carycha",
    license='MIT License',
    description="自己用的小工具",
    packages=find_packages(),
    py_modules=['cary_package'],
    package_data={'cary_package': ['static/jieba_dict/*.*']},
    include_package_data=True,
    install_requires=[
        # 'jieba',
        'jieba-fast>=0.53',
        'pyahocorasick>=2.1.0',
        'ndjson',
        'elasticsearch>=8.13.0',
        'redis',
        'pymysql',
        'pandas',
        'google>=3.0.0',
        'unicodedata2>=15.1.0',
        'requests',
        'scikit-learn',
        'httpx',
        'numpy',
        'scipy',
    ],
)
