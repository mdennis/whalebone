#!/usr/bin/env python

from __future__ import with_statement

import os
import sys
import yaml
import boto
import time
import copy
import shlex
import pickle
import threading
import subprocess
from optparse import OptionParser
from boto.exception import EC2ResponseError

class WhaleboneManager:
    VALID_COMMANDS = set([
        'run',
        'ring',
        'list',
        'stress',
        'list_public_dns',
        'list_private_dns',
        'launch',
        'wait_for_launch',
        'terminate',
        'wait_for_terminate',
        'start',
        'stop',
        'cleardata',
        'push_config',
        'push_client_config',
        'install_client',
        'download_cassandra',
        'wait_for_ssh',
        'full_upgrade',
        'reboot',
        'build_raid',
        'mkdirs',
        'push_local_cassandra',
        'import_schema',
        'push_pub_key'
    ])

    def __init__(self, options, args):
        self.options = options
        self.args = args
        options.cluster_name = args[0]
        options.command = args[1]
        options.remote_cluster_name = None
        if options.command == 'push_client_config':
            options.remote_cluster_name = args[2]
        elif options.command == 'stress':
            options.stress_options = args[2]
        elif options.command == 'run':
            options.run = args[2]
        elif options.command == 'push_local_cassandra':
            options.local_cassandra_dir = args[2]
        elif options.command == 'push_pub_key':
            options.key_to_push = args[2]
        
        self.ec2_conn = boto.connect_ec2()
        self.base_ssh_command = [
          'ssh', '-i', self.options.credentials, '-l', 
          self.options.user, '-o', 'StrictHostKeyChecking=no'
        ]
        self.base_pssh_command = [
            'parallel-ssh', '-i', '-O', 'StrictHostKeyChecking=no',
            '-h', self._public_dns_names_path(), '-t', '-1',
            '-l', self.options.user, '-x', '-i %s' % self.options.credentials
        ]
        self.base_pscp_command = [
            'parallel-scp', '-O', 'StrictHostKeyChecking=no',
            '-h', self._public_dns_names_path(),
            '-l', self.options.user, '-x', '-i %s' % self.options.credentials
        ]

    def execute(self):
        if self.options.command in self.VALID_COMMANDS:
            return getattr(self, self.options.command)()
        raise NotImplementedError('unknown command [%s]' % self.options.command)

    def _ensure_cluster_path(self):
        path = self._cluster_path()
        if not os.path.exists(path):
            os.makedirs(path)

    def _cluster_path(self, for_cluster=None):
        if for_cluster == None:
            for_cluster = self.options.cluster_name
        return os.path.join(os.path.expanduser('~/.whalebone/'), for_cluster)

    def _instances_path(self):
        return os.path.join(self._cluster_path(), 'instances')

    def _public_dns_names_path(self, for_cluster=None):
        return os.path.join(self._cluster_path(for_cluster), 'boxen.public')

    def _private_dns_names_path(self, for_cluster=None):
        return os.path.join(self._cluster_path(for_cluster), 'boxen.private')

    def _save_instances(self, instances):
        self._ensure_cluster_path()
        with open(self._instances_path(), 'w') as outfile:
            pickle.dump([i.id for i in instances], outfile)
        with open(self._public_dns_names_path(), 'w') as outfile:
            outfile.write('\n'.join([i.public_dns_name for i in instances]))
        with open(self._private_dns_names_path(), 'w') as outfile:
            outfile.write('\n'.join([i.private_dns_name for i in instances]))

    def _instances(self):
        path = self._instances_path()
        if not os.path.exists(path):
            return []

        instances = []
        instance_ids = []
        with open(path, 'r') as infile:
            instance_ids = pickle.load(infile)

        while len(instances) != len(instance_ids):
            try:
                result_set = self.ec2_conn.get_all_instances(instance_ids=instance_ids)
                for reservation in result_set:
                    instances.extend(reservation.instances)
            except EC2ResponseError, ec2re:
                instances = []
                print ec2re
                time.sleep(0.5)

        return instances
        
    def list(self):
        for i in self._instances():
            print '%s %s %s' % (i.id, i.public_dns_name, i.private_dns_name)

    def list_public_dns(self):
        for i in self._instances():
            print i.public_dns_name

    def list_private_dns(self):
        for i in self._instances():
            print i.private_dns_name

    def launch(self):
        self.terminate()
        image = self.ec2_conn.get_image(self.options.image_id)
        with open(self.options.setup_script, 'r') as f:
            reservations = image.run(self.options.cluster_size, self.options.cluster_size, self.options.keypair, 
                                     instance_type=self.options.instance_type, user_data=f.read())
        self._save_instances(reservations.instances)
        self.wait_for_launch()
        self.install_client()
        self.download_cassandra()
        self.push_config()


    def install_client(self):
        print 'installing whalebone client'
        subprocess.check_call(self.base_pssh_command + ['mkdir', '-p', '/home/%s/bin' % self.options.user])
        subprocess.check_call(self.base_pscp_command + [os.path.join(sys.path[0], 'whalebone_client.py'), '/home/%s/bin' % self.options.user])
        subprocess.check_call(self.base_pssh_command + ['chmod', 'a+x', '/home/%s/bin/whalebone_client.py' % self.options.user]) 
        subprocess.check_call(self.base_pscp_command + [self._public_dns_names_path(), '/home/%s/boxen.public' % self.options.user])
        subprocess.check_call(self.base_pscp_command + [self._private_dns_names_path(), '/home/%s/boxen.private' % self.options.user])
        
    def download_cassandra(self):
        print 'installing cassandra'
        args = ['/home/%s/bin/whalebone_client.py' % self.options.user, 'download_cassandra', '--download_version', self.options.cassandra_version]
        subprocess.check_call(self.base_pssh_command + args)

    def build_raid(self):
        print 'building raid'
        cmds = [
            ['sudo', 'apt-get', 'install', 'mdadm', '--no-install-recommends'],
            ['sudo', 'umount', '/dev/sdb'],
            ['sudo', 'dd', 'if=/dev/zero', 'of=/dev/sdb', 'bs=4096', 'count=1024'],
            ['sudo', 'dd', 'if=/dev/zero', 'of=/dev/sdc', 'bs=4096', 'count=1024'],
            ['sudo', 'dd', 'if=/dev/zero', 'of=/dev/sdd', 'bs=4096', 'count=1024'],
            ['sudo', 'dd', 'if=/dev/zero', 'of=/dev/sde', 'bs=4096', 'count=1024'],
            ['sudo', 'partprobe'],
            ['sudo', 'mdadm', '--create', '/dev/md0', '--level=0', '--raid-devices=4', '--run', '/dev/sdb', '/dev/sdc', '/dev/sdd', '/dev/sde'],
            ['sudo', 'mkfs.ext3', '/dev/md0'],
            ['sudo', 'sed', '-i', '-e', 's/sdb/md0/', '/etc/fstab'],
            ['sudo', 'mount', '/dev/md0', '/mnt'],
            ['sudo', 'chmod', 'o+w', '/etc/mdadm/mdadm.conf'],
            ['sudo', 'mdadm', '--examine', '--scan', '--config=/etc/mdadm/mdadm.conf', '>>', '/etc/mdadm/mdadm.conf']
        
        ]
        for args in cmds:
            subprocess.check_call(self.base_pssh_command + args)
        self.mkdirs()

    def mkdirs(self):
        cmds = [
            ['sudo', 'mkdir', '-p', '-m', '0755', '/var/log/cassandra', '/var/run/cassandra', '/mnt/cassandra-data',
             '/mnt/cassandra-gclog', '/mnt/cassandra-caches', '/mnt/cassandra-commitlogs'],
            ['sudo', 'chown', 'ubuntu:ubuntu', '/var/log/cassandra', 
             '/var/run/cassandra', '/mnt/cassandra-data', '/mnt/cassandra-gclog', 
             '/mnt/cassandra-caches', '/mnt/cassandra-commitlogs'],
            ['sudo', 'mkdir', '-p', '/mnt/public'],
            ['sudo', 'chown', 'ubuntu:ubuntu', '/mnt/public'],
            ['sudo', 'chmod', 'a+rwx', '/mnt/public']
        ]
        for args in cmds:
            subprocess.check_call(self.base_pssh_command + args)

    def full_upgrade(self):
        print 'running full-upgrade'
        subprocess.check_call(self.base_pssh_command + ['sudo', 'aptitude', '-y', 'full-upgrade'])
        
    def run(self):
        subprocess.check_call(self.base_pssh_command + [self.options.run])

    def reboot(self):
        print 'rebooting boxen'
        subprocess.check_call(self.base_pssh_command + ['sudo', 'reboot'])

    def wait_for_ssh(self):
        boxen = set([i.public_dns_name for i in self._instances()])
        while len(boxen) > 0:
            time.sleep(1)
            print 'waiting for ssh on %s boxen - %s' % (len(boxen), boxen)
            processes = []
            for b in boxen:
                processes.append((subprocess.Popen(self.base_ssh_command + [b, 'exit 0']),b))
            for p,b in processes:
                if p.wait() == 0:
                    print 'ssh is up on %s' % b
                    boxen.remove(b)

    def wait_for_launch(self):
        self.wait_for_state('running')
        self.wait_for_ssh()
        print 'waiting for setup to complete'
        waitcmd = 'while true; do if [ -f /setup_complete ]; then exit 0; fi; sleep 2; done'
        subprocess.check_call(self.base_pssh_command + [waitcmd])

    def terminate(self):
        self._ensure_cluster_path()
        instances = self._instances()
        if len(instances) > 0:
            self.ec2_conn.terminate_instances(instance_ids=[i.id for i in instances])
            self.wait_for_terminate()
            os.remove(self._instances_path())
            os.remove(self._public_dns_names_path())
            os.remove(self._private_dns_names_path())
        
    def wait_for_terminate(self):
        self.wait_for_state('terminated')

    def wait_for_state(self, state):
        done = set()
        instances = self._instances()

        print 'waiting for %s boxen to be %s' % (len(instances), state)
        while len(done) != len(instances):
            time.sleep(2)
            for i in instances:
                try:
                    i.update()
                    if i.id not in done and (i.state == state or i.state == 'terminated'):
                        done.add(i.id)
                        print '%s (%s) is %s' % (i.public_dns_name, i.id, state)
                except Exception, e:
                    print e
        self._save_instances(instances)

    def start(self):
        cmd0 = 'sudo mkdir -p -m 0755 /var/run/cassandra && sudo chown ubuntu:ubuntu /var/run/cassandra'
        cmd1 = '/usr/local/apache-cassandra/bin/cassandra -p /var/run/cassandra/cassandra.pid'
        subprocess.check_call(self.base_pssh_command + [cmd0])
        subprocess.check_call(self.base_pssh_command + [cmd1])

    def stop(self):
        cmd = 'kill -9 `cat /var/run/cassandra/cassandra.pid` && rm /var/run/cassandra/cassandra.pid'
        subprocess.check_call(self.base_pssh_command + [cmd])

    def ring(self):
        cmd = '/usr/local/apache-cassandra/bin/nodetool -h 127.0.0.1 ring'
        subprocess.check_call(self.base_pssh_command + [cmd])

    def stress(self):
        cmd = 'cd /usr/local/apache-cassandra-src && contrib/py_stress/stress.py --keep-going -i 1 -D %s %s' % ('~/remote_boxen.private', self.options.stress_options)
        subprocess.check_call(self.base_pssh_command + [cmd])

    def cleardata(self):
        cmd = 'rm -rf /mnt/cassandra-commitlogs/* /mnt/cassandra-data/* /mnt/cassandra-caches/* /var/log/cassandra/* /mnt/cassandra-gclog/*'
        subprocess.check_call(self.base_pssh_command + [cmd])

    def push_config(self):
        print 'pushing cassandra config'
        subprocess.check_call(self.base_pscp_command + [self.options.cassandra_config, '/usr/local/apache-cassandra/conf/cassandra.yaml'])
        subprocess.check_call(self.base_pscp_command + [self.options.cassandra_env, '/usr/local/apache-cassandra/conf/cassandra-env.sh'])
        subprocess.check_call(self.base_pssh_command + ['/home/%s/bin/whalebone_client.py' % self.options.user, 'modify_config'])

    def import_schema(self):
        print 'importing schema'
        args = [self._instances()[0].public_dns_name, '/usr/local/apache-cassandra/bin/schematool', '127.0.0.1', '8080', 'import']
        subprocess.check_call(self.base_ssh_command + args)

    def push_client_config(self):
        print 'pushing client config'
        pubargs = [self._public_dns_names_path(self.options.remote_cluster_name), '/home/%s/remote_boxen.public' % self.options.user]
        privargs = [self._private_dns_names_path(self.options.remote_cluster_name), '/home/%s/remote_boxen.private' % self.options.user]
        subprocess.check_call(self.base_pscp_command + pubargs)
        subprocess.check_call(self.base_pscp_command + privargs)

    def push_local_cassandra(self):
        print 'pushing local cassandra %s' % self.options.local_cassandra_dir
        cmd = ['parallel-rsync', '-raz', '-X', '--delete', '-X', '--exclude', '-X', '*.git', '-X', 
               '--delete-excluded', '-h', self._public_dns_names_path(), '-l', self.options.user, 
               "%s/"%self.options.local_cassandra_dir, '/usr/local/apache-cassandra']
        subprocess.check_call(cmd)

    def push_pub_key(self):
        with open(self.options.key_to_push) as pubkey:
            cmd = self.base_pssh_command + ['-I', 'cat >> .ssh/authorized_keys']
            subprocess.check_call(cmd, stdin=pubkey)

def parse_args():
    parser = OptionParser(usage='usage: %prog cluster_name command [options]')

    parser.add_option('--cluster_size', dest='cluster_size', default=1,
                      help='The size of the cluster', metavar='CLUSTER_SIZE')

    parser.add_option('--keypair', dest='keypair', default='awsmfd',
                      help='The name of the keypair to launch this instance with', metavar='KEYPAIR_NAME')
 
    parser.add_option('--image_id', dest='image_id', default='ami-08f40561',
                      help='the AWS AMI id for the Cassandra boxen', metavar='AWS_AMI_ID')

    parser.add_option('--instance_type', dest='instance_type', default='m1.xlarge',
                      help='The AWS instance type (default m1.xlarge)', metavar='AWS_INSTANCE_TYPE')

    parser.add_option('--credentials', dest='credentials', default='/home/mdennis/.ssh/awsmfd.pem',
                      help='The SSH credentials (.pem) to use to connect to the instances')

    parser.add_option('--user', dest='user', default='ubuntu',
                      help='The SSH username', metavar='USERNAME')

    parser.add_option('--setup_script', dest='setup_script',
                      default=os.path.expanduser('~/.whalebone/setup_script.sh'),
                      help='The setup script to run on the EC2 instance on first boot', metavar='PATH_TO_SETUP_SCRIPT')

    parser.add_option('--cassandra_config', dest='cassandra_config',
                      default=os.path.expanduser('~/.whalebone/cassandra.yaml'),
                      help='The cassandra.yaml file to load on each instance', metavar='PATH_TO_CASSANDRA_DOT_YAML')

    parser.add_option('--cassandra_env', dest='cassandra_env',
                      default=os.path.expanduser('~/.whalebone/cassandra-env.sh'),
                      help='The cassandra-env.sh file to load on each instance', metavar='PATH_TO_CASSANDRA_DASH_ENV_DOT_SH')

    parser.add_option('--cassandra_version', dest='cassandra_version', default='0.7.0-rc2',
                      help='The cassandra version to download', metavar='CASSANDRA_VERSION')

    (options, args) = parser.parse_args()
    if len(args) < 2 or len(args) > 3:
        parser.print_help()
        exit(1)
        
    if all([
        len(args) == 3,
        args[1] != 'push_client_config',
        args[1] != 'stress',
        args[1] != 'run',
        args[1] != 'push_local_cassandra',
        args[1] != 'push_pub_key'
    ]):
        parser.print_help()
        exit(1)

    return (options, args)

def main():
    manager = WhaleboneManager(*parse_args())
    manager.execute()

if __name__ == '__main__':
    main()

