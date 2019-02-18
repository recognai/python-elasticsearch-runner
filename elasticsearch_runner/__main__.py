import os
import atexit
import plac

from elasticsearch_runner.runner import ElasticsearchRunner, ES_DEFAULT_VERSION


@plac.annotations(
    version=plac.Annotation('Elasticsearch engine version', kind='option', abbrev='v')
)
def main(version: str = ES_DEFAULT_VERSION):
    runner = ElasticsearchRunner(install_path=os.getcwd(), version=version)

    runner.install()
    runner.run()

    atexit.register(runner.stop)
    runner.wait_process()


if __name__ == '__main__':
    plac.call(main)
