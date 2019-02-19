## Elasticsearch runner 

The python-elasticsearch-runner contains a standalone Python runner for Elasticsearch. This is intended
for transient and lightweight usage such as small integration tests or local environments.

The runner takes about 10 sec. to start so it should be a part of at least module level setup/teardown in
order to minimize test run time.

The following code sets up the runner instance at module level with nosetests if placed in __init__.py:

```python
from elasticsearch_runner.runner import ElasticsearchRunner

es_runner = ElasticsearchRunner()

def setup():
    es_runner.install()
    es_runner.run()
    es_runner.wait_for_green()

def teardown():
    if es_runner and es_runner.is_running():
        es_runner.stop()
```

The runner instance can then be queried for the port number when connecting:

```python
es = Elasticsearch(hosts=['localhost:%d' % es_runner.es_state.port])
```

### Running as module
You can also launch a local es instance by launching the module in your terminal:

````bash
python -m elasticsearch_runner
````

This command build up an elasticsearch installation in the current directory. You can select the 
elasticsearch version passing `--version` command

````bash
>python -m elasticsearch_runner -h

usage: __main__.py [-h] [-v 6.4.3] {start,stop,terminate}

positional arguments:
  {start,stop,terminate}
                        Start/stop or terminate engine

optional arguments:
  -h, --help            show this help message and exit
  -v 6.4.3, --version 6.4.3
                        Elasticsearch engine version

````


### Some details
Should run with python 2.7. 3.3 and 3.4
By default, elasticsearch version 6.4.3 is used, and everything is installed into HOME/.elasticsearch_runner (most systems) or APP_DATA/elasticsearch_runner (windows) folder.


```python
es_runner = ElasticsearchRunner(version=1.0.0, install_path=/var/test/)
```

The elasticsearch runner accepts parameters for elasticsearch version and install path. 
The install path is where the Elasticsearch software package and data storage will be kept.
Install path can also be provided as the environment variable 'elasticsearch-runner-install-path', and if set will override the install_path parameter.

