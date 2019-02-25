from setuptools import setup

setup(
    name="elasticsearch-runner",
    version="0.1",
    packages=[
        "elasticsearch_runner.resources",
        "elasticsearch_runner",
        "elasticsearch_runner.test",
    ],
    url="https://github.com/recognai/python-elasticsearch-runner",
    author="Francisco Aranda",
    author_email="francisco@recogn.ai",
    description="Lightweight runner for transient Elasticsearch instances, f.ex for testing. or local environments",
    install_requires=[
        "PyYAML",
        "elasticsearch",
        "requests",
        "psutil",
        "lxml",
        "plac",
        "tqdm",
    ],
    package_data={"elasticsearch_runner": ["resources/*.*"]},
)
