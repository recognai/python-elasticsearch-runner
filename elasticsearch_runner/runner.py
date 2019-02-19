import errno
import json
import logging
import os
import pathlib
import re
import shutil
import sys
import glob
import time
from collections import namedtuple
from shutil import copyfile, rmtree
from subprocess import Popen
from time import sleep, clock
from typing import Optional
from zipfile import ZipFile

from tqdm import tqdm

_logger = logging.getLogger(__name__)

PY3 = sys.version_info > (3,)
if PY3:
    import urllib.request, urllib.parse, urllib.error
    import urllib.parse
else:
    from urlparse import urlparse

from psutil import Process, NoSuchProcess
import requests

from elasticsearch_runner.configuration import (
    serialize_config,
    generate_config,
    generate_cluster_name,
    package_path,
)

"""
Class for starting, stopping and managing an Elasticsearch instance from within a Python process.

Intended for testing and other lightweight purposes with transient data.

TODO Faster Elasticsearch startup.
"""

ES_DEFAULT_VERSION = "6.4.3"

ES_URLS = {
    "1.7.2": "https://download.elastic.co/elasticsearch/elasticsearch/elasticsearch-1.7.2.zip",
    "2.0.0": "https://download.elasticsearch.org/elasticsearch/release/org/elasticsearch/distribution/zip/elasticsearch/2.0.0/elasticsearch-2.0.0.zip",
    ES_DEFAULT_VERSION: "https://artifacts.elastic.co/downloads/elasticsearch/elasticsearch-{}.zip".format(
        ES_DEFAULT_VERSION
    ),
}

ES_DEFAULT_URL_LOCATION = "https://artifacts.elastic.co/downloads/elasticsearch"
ES1x_DEFAULT_URL_LOCATION = (
    "https://download.elastic.co/elasticsearch/elasticsearch/elasticsearch"
)
ES2x_DEFAULT_URL_LOCATION = "https://download.elasticsearch.org/elasticsearch/release/org/elasticsearch/distribution/zip/elasticsearch/"


def fn_from_url(url):
    """
    Extract the final part of an url in order to get the filename of a downloaded url.

    :param url: url string
    :type url : str|unicode
    :rtype : str|unicode
    :return: url filename part
    """

    if PY3:
        parse = urllib.parse.urlparse(url)
    else:
        parse = urlparse(url)
    return os.path.basename(parse.path)


def download_file(url, dest_path):
    """
    Download the file pointed to by the url to the path specified .
    If the file is already present at the path it will not be downloaded and the path to this file
    is returned.

    :param url: url string pointing to the file
    :type url : str|unicode
    :param dest_path: path to location where the file will be stored locally
    :type dest_path : str|unicode
    :rtype : str|unicode
    :return: path to the downloaded file
    """
    if not os.path.exists(dest_path):
        os.makedirs(dest_path)

    fn = fn_from_url(url)
    full_fn = os.path.join(dest_path, fn)

    if os.path.exists(full_fn):
        _logger.info("Dataset archive %s already exists in %s ..." % (fn, dest_path))
    else:
        _logger.info("Downloading files from {}".format(url))
        r = requests.get(url, stream=True)
        with open(full_fn, "wb") as f:
            progress_bar = tqdm(unit="B", total=int(r.headers["Content-Length"]))
            for chunk in r.iter_content(chunk_size=1024):
                if chunk:  # filter out keep-alive new chunks
                    progress_bar.update(len(chunk))
                    f.write(chunk)
                    f.flush()
            progress_bar.close()

    return full_fn


def check_java():
    """
    Simple check for Java availability on the local system.

    :rtype : bool
    :return: True if Java available on the command line
    """
    return os.system("java -version") == 0


def process_exists(pid):
    """
    Check if there is a process with this PID.

    :param pid: Process ID
    :type pid: int
    :rtype : bool
    :return: True if the process exists, False otherwise
    """
    if os.name == "nt":
        # TODO something more solid on windows?
        try:
            return Process(pid).status() == "running"
        except NoSuchProcess:
            return False
    else:
        try:
            os.kill(pid, 0)
        except OSError:
            return False

        return True


def parse_es_log_header(log_file, limit=200):
    """
    Look at Elasticsearch log for startup messages containing system information. The log is read until the starting
    message is detected or the number of lines read exceed the limit.
    The log file must be open fir reading and at the desired position, ie. the end to read incoming log lines.

    :param log_file: open for reading file instance for the log file at the correct position
    :type log_file: FileIO
    :param limit: max lines to read before returning
    :type limit: int
    :rtype : (int|None, int|None)
    :return: A tuple with the Elasticsearch instance PID and REST endpoint port number, ie. (pid, port)
    """
    line = log_file.readline()
    server_pid = None
    es_port = 9200
    count = 0

    while count < limit:
        count += 1
        line = line.strip()

        if line == "":
            sleep(0.1)

        m = re.search("pid\[(\d+)\]", line)
        if m:
            server_pid = int(m.group(1))

        m = re.search(r"http.*publish_address.*:(\d+)", line.lower())
        if m:
            es_port = int(m.group(1))

        m = re.search(r".*started.*", line)
        if m:
            return server_pid, es_port

        line = log_file.readline()

    _logger.warning(
        "Read more than %d lines while parsing Elasticsearch log header. Giving up ..."
        % limit
    )

    return server_pid, es_port


# tuple holding information about the current Elasticsearch process
ElasticsearchState = namedtuple(
    "ElasticsearchState", "server_pid wrapper_pid port config_fn"
)


def fetch_pid_from_pid_file(pid_path: str) -> Optional[int]:
    try:
        with open(pid_path) as pid_file:
            return int(pid_file.readline())
    except Exception:
        return None


class ElasticsearchRunner:
    """
    Runs a basic single node Elasticsearch instance for testing or other lightweight purposes.
    """

    def __init__(self, install_path=None, transient=False, version=None):
        """
        :param version: Elasticsearch version to run. Defaults to 2.1.0
        :type version: string
        :param install_path: The path where the Elasticsearch software package and data storage will be kept.
        If no install path set, installs into APPDATA (windows)or  HOME/.elasticsearch_runner (other)
        Install_path can be provided as the environment variable 'elasticsearch-runner-install-path'
        If environment variable provided it will override install_path parameter
        :type install_path: str|unicode
        :param transient: Not implemented.
        :type transient: bool
        """
        if os.getenv("elasticsearch-runner-install-path"):
            install_path = os.getenv("elasticsearch-runner-install-path")

        if install_path:
            self.install_path = install_path
        else:
            if os.name == "nt":
                self.install_path = os.path.join(
                    os.getenv("APPDATA"), "elasticsearch_runner", "embedded-es"
                )
            else:
                self.install_path = os.path.join(
                    os.getenv("HOME"), ".elasticsearch_runner", "embedded-es"
                )
        if version:
            self.version = version
        else:
            self.version = ES_DEFAULT_VERSION
        self.version_folder = "elasticsearch-%s" % self.version
        self.transient = transient
        self.es_state = None
        self.es_config = None

        if not check_java():
            _logger.error("Java not installed. Elasticsearch won't be able to run ...")

    def install(self):
        """
        Download and install the Elasticsearch software in the install path. If already downloaded or installed
        those steps are skipped.

        :rtype : ElasticsearchRunner
        :return: The instance called on.
        """

        def extension_for_os_name(sys_platform: str) -> str:
            return "zip"

        if self.version in ES_URLS:
            download_url = ES_URLS[self.version]
        else:
            mayor, _, _ = self.version.split(".")

            if mayor == "1":
                download_url = "%s-%s.zip" % (ES1x_DEFAULT_URL_LOCATION, self.version)
            elif mayor == "2":
                download_url = "%s%s/elasticsearch-%s.zip" % (
                    ES2x_DEFAULT_URL_LOCATION,
                    self.version,
                    self.version,
                )
            else:
                download_url = "{}/elasticsearch-{}.zip".format(
                    ES_DEFAULT_URL_LOCATION, self.version
                )

        es_archive_fn = download_file(download_url, self.install_path)

        es_home = os.path.join(self.install_path, self.version_folder)
        if not os.path.exists(es_home):
            with ZipFile(es_archive_fn, "r") as z:
                z.extractall(self.install_path)

        # insert basic config file
        copyfile(
            os.path.join(
                package_path(),
                "elasticsearch_runner",
                "resources",
                "embedded_elasticsearch.yml",
            ),
            os.path.join(es_home, "config", "elasticsearch.yml"),
        )

        # WORKAROUND: remove x-pack modules for avoid execution permission problems

        for x_path_module in glob.glob(os.path.join(es_home, "modules", "x-pack*")):
            shutil.rmtree(x_path_module)

        return self

    def run(self):
        """
        Start the elasticsearch server. Running REST port and PID is stored in the es_state field.

        :rtype : ElasticsearchRunner
        :return: The instance called on.
        """
        if self.is_running():
            _logger.warning("Elasticsearch already running ...")
            return self

        # generate and insert Elasticsearch configuration file with transient data and log paths
        cluster_name = generate_cluster_name()
        cluster_path = pathlib.Path(
            os.path.join(self.install_path, "%s-%s" % (self.version, cluster_name))
        )

        es_data_dir = pathlib.Path(os.path.join(cluster_path, "data"))
        es_config_dir = pathlib.Path(os.path.join(cluster_path, "config"))
        es_log_dir = pathlib.Path(os.path.join(cluster_path, "log"))
        pid_path = self.__get_pid_file(cluster_path)

        self.es_config = generate_config(
            cluster_name=cluster_name,
            data_path=str(es_data_dir.absolute()),
            log_path=str(es_log_dir.absolute()),
        )
        config_fn = os.path.join(es_config_dir, "elasticsearch.yml")

        try:
            cluster_path.mkdir(parents=True, exist_ok=True)
            es_log_dir.mkdir(parents=True, exist_ok=True)
            es_data_dir.mkdir(parents=True, exist_ok=True)
            es_config_dir.mkdir(parents=True, exist_ok=True)
        except OSError as exception:
            if exception.errno != errno.EEXIST:
                raise
        with open(config_fn, "w") as f:
            serialize_config(f, self.es_config)

        for from_resource, to_resource in [
            ("embedded_logging.yml", "logging.yml"),
            ("jvm.options", "jvm.options"),
            ("log4j.properties", "log4j2.properties"),
        ]:
            copyfile(
                os.path.join(
                    package_path(), "elasticsearch_runner", "resources", from_resource
                ),
                os.path.join(es_config_dir, to_resource),
            )

        es_log_fn = os.path.join(es_log_dir, "%s.log" % cluster_name)
        # create the log file if it doesn't exist yet. We need to open it and seek to to the end before
        # sniffing out the configuration info from the log.

        server_pid_from_file = fetch_pid_from_pid_file(pid_path)
        if not server_pid_from_file:

            open(es_log_fn, "w").close()
            runcall = self._es_wrapper_call(os.name)

            mayor, _, _ = self.version.split(".")
            if int(mayor) < 5:
                runcall.extend(
                    [
                        "-Des.path.conf=%s" % es_config_dir,
                        "-Des.path.logs=%s" % es_log_dir,
                    ]
                )

            call_args = ["-p", pid_path]
            runcall.extend(call_args)
            Popen(runcall, env={**os.environ, **dict(ES_PATH_CONF=es_config_dir)})
            time.sleep(3)
            server_pid_from_file = fetch_pid_from_pid_file(pid_path)

        self.es_state = ElasticsearchState(
            wrapper_pid=None,
            server_pid=server_pid_from_file,
            port=9200,
            config_fn=config_fn,
        )
        return self

    @staticmethod
    def __get_pid_file(cluster_path):
        return os.path.join(cluster_path, ".pid")

    def _es_wrapper_call(self, os_name):
        """
        :param os_name: OS identifier as returned by os.name
        :type os_name: str|unicode
        :rtype : list[str|unicode]
        :return:
        """

        if os_name == "nt":
            es_bin = [
                os.path.join(
                    self.install_path, self.version_folder, "bin", "elasticsearch.bat"
                )
            ]
        else:
            es_bin = [
                "/bin/sh",
                os.path.join(
                    self.install_path, self.version_folder, "bin", "elasticsearch"
                ),
            ]

        return es_bin

    def stop(self, delete_transient: bool = True):
        """
        Stop the Elasticsearch server.

        :rtype : ElasticsearchRunner
        :return: The instance called on.
        """

        if self.is_running():
            pid = self.__es_pid()

            server_proc = Process(pid)
            server_proc.terminate()
            server_proc.wait()

            if process_exists(pid):
                _logger.warning(
                    "Failed to stop Elasticsearch server process PID %d ..." % pid
                )

            # delete transient directories
            if delete_transient:
                if "path" in self.es_config:
                    if "log" in self.es_config["path"]:
                        log_path = self.es_config["path"]["log"]
                        _logger.info("Removing transient log path %s ..." % log_path)
                        rmtree(log_path)

                    if "data" in self.es_config["path"]:
                        data_path = self.es_config["path"]["data"]
                        _logger.info("Removing transient data path %s ..." % data_path)
                        rmtree(data_path)

                # delete temporary config file
                if os.path.exists(self.es_state.config_fn):
                    _logger.info(
                        "Removing transient configuration file %s ..."
                        % self.es_state.config_fn
                    )
                    os.remove(self.es_state.config_fn)

            self.es_state = None
            self.es_config = None
        else:
            _logger.warning("Elasticsearch is not running ...")
            self.es_state = None
            self.es_config = None

        return self

    def is_running(self):
        """
        Checks if the instance has a running server process and that thhe process exists.

        :rtype : bool
        :return: True if the servier is running, False if not.
        """
        pid = self.__es_pid()
        return not pid is None and process_exists(pid)

    def __es_pid(self):
        if self.es_state and self.es_state.server_pid:
            pid = self.es_state.server_pid
        else:
            pid = self.__pid_from_file()
        return pid

    def __pid_from_file(self) -> Optional[int]:
        try:
            cluster_path = generate_cluster_name()
            pid_path = self.__get_pid_file(cluster_path)
            pid = fetch_pid_from_pid_file(pid_path)
            return pid
        except:
            return None

    def wait_for_green(self, timeout=1.0):
        """
        Check if cluster status is green and wait for it to become green if it's not.
        Run after starting the runner to ensure that the Elasticsearch instance is ready.

        :param timeout: The time to wait for green cluster response in seconds.
        :type timeout: int|long|float
        :rtype : ElasticsearchRunner
        :return:
        """
        if not self.es_state:
            _logger.warning("Elasticsearch runner is not started ...")
            return self

        if self.es_state.port is None:
            _logger.warning("Elasticsearch runner not properly started ...")
            return self
        end_time = clock() + timeout
        health_resp = requests.get(
            "http://localhost:%d/_cluster/health" % self.es_state.port
        )
        health_data = json.loads(health_resp.text)

        while health_data["status"] != "green":
            if clock() > end_time:
                _logger.error(
                    "Elasticsearch cluster failed to turn green in %f seconds, current status is %s ..."
                    % (timeout, health_data["status"])
                )

                return self

            health_resp = requests.get(
                "http://localhost:%d/_cluster/health" % self.es_state.port
            )
            health_data = json.loads(health_resp.text)

        return self

    def wait_process(self, timeout: Optional[int] = None):
        if self.is_running():
            pid = self.__es_pid()
            process = Process(pid)
            process.wait(timeout=timeout)
