from setuptools import setup

setup(
    name='LucidMQ-py',
    version='0.1.0',    
    description='LucidMQ Client code',
    url='https://github.com/lucidmq/lucidmq',
    license='MIT',
    #packages=['lucidmq-py'],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License"
    ],
    install_requires=['msgpack'],
    python_requires='>=3.8',
    include_package_data=True
)