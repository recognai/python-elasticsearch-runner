import os

import plac

from elasticsearch_runner.runner import ElasticsearchRunner, ES_DEFAULT_VERSION


@plac.annotations(
    command=plac.Annotation(
        "Start/stop or terminate engine", choices=["start", "stop", "terminate"]
    ),
    version=plac.Annotation("Elasticsearch engine version", kind="option", abbrev="v"),
)
def main(command: str, version: str = ES_DEFAULT_VERSION):
    runner = ElasticsearchRunner(install_path=os.getcwd(), version=version)
    runner.install()
    runner.run()

    if command == "stop" or command == "terminate":
        runner.stop(delete_transient="terminate" == command)


if __name__ == "__main__":
    plac.call(main)
