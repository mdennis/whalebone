#!/usr/bin/env python

from __future__ import with_statement

class WhaleboneClient:
    VALID_COMMANDS = set([
      'modify_config',
      'download_cassandra'
    ])

    def __init__(self):
        from optparse import OptionParser
        parser = OptionParser()
        parser.add_option("--download_version", dest="download_version",
                          default='0.7.0-rc2', metavar="CASSANDRA_VERSION",
                          help="The cassandra version to download")
        (self.options, self.args) = parser.parse_args()
        self.command = self.args[0]
        
    def _instances(self):
        import os
        with open(os.path.expanduser("~/boxen.private")) as instances:
            return [i.strip() for i in instances.readlines() if len(i.strip()) > 0]

    def _get_token(self):
        import socket
        instances = self._instances()
        return str((instances.index(socket.getfqdn()) * (2 ** 127 - 1) / len(instances)))

    def _seed_instances(self):
        instances = self._instances()
        return instances[::max(len(instances)/3, 1)]
        
    def modify_config(self):
        import yaml
        import socket
        with open('/usr/local/apache-cassandra/conf/cassandra.yaml', 'r') as config_file:
            config = yaml.load(config_file)

        config['initial_token'] = self._get_token()
        config['seeds'] = self._seed_instances()
        config['listen_address'] = socket.getfqdn()
        config['rpc_address'] = socket.getfqdn()

        with open('/usr/local/apache-cassandra/conf/cassandra.yaml', 'w') as config_file:
            config_file.write(yaml.safe_dump(config))

    def download_cassandra(self):
        import shutil
        import urllib
        import subprocess
        subprocess.check_call(['sudo', 'rm', '-rf', '/usr/local/apache-cassandra*'])
        binsrc = 'apache-cassandra-%s-bin.tar.gz' % self.options.download_version
        srcsrc = 'apache-cassandra-%s-src.tar.gz' % self.options.download_version
        bindest = '/tmp/%s' % binsrc
        srcdest = '/tmp/%s' % srcsrc
        bindir = '/usr/local/apache-cassandra-%s' % self.options.download_version
        srcdir = '/usr/local/apache-cassandra-%s-src' % self.options.download_version
        urllib.urlretrieve('http://apache.mirrors.pair.com/cassandra/0.7.0/%s'%binsrc, bindest)
        urllib.urlretrieve('http://apache.mirrors.pair.com/cassandra/0.7.0/%s'%srcsrc, srcdest)
        subprocess.check_call(['sudo', 'tar', '-C', '/usr/local', '-xzf', bindest])
        subprocess.check_call(['sudo', 'tar', '-C', '/usr/local', '-xzf', srcdest])
        subprocess.check_call(['sudo', 'chown', '-R', 'ubuntu:ubuntu', bindir])
        subprocess.check_call(['sudo', 'chown', '-R', 'ubuntu:ubuntu', srcdir])
        subprocess.check_call(['sudo', 'ln', '-sf', bindir, '/usr/local/apache-cassandra'])
        subprocess.check_call(['sudo', 'ln', '-sf', srcdir, '/usr/local/apache-cassandra-src'])
        subprocess.check_call(['ant', '-q', '-f', '/usr/local/apache-cassandra-src/build.xml', 'gen-thrift-py'])
        subprocess.check_call(['chmod', 'a+x', '%s/contrib/py_stress/stress.py' % srcdir])
        
    def run(self):
        if self.command in self.VALID_COMMANDS:
            return getattr(self, self.command)()
        raise NotImplementedError("unknown command [%s]" % command)
        
if  __name__ == "__main__":
    WhaleboneClient().run()

