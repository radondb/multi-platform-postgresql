import paramiko
from typing import Dict, TypedDict, TypeVar, Optional, List, Optional, Callable, Tuple, Any
from constants import (
    AUTOFAILOVER,
    POSTGRESQL,
)

LabelType = Dict[str, str]


class InstanceConnectionMachine:

    def __init__(self, host: str, port: int, username: str, password: str,
                 ssh: paramiko.SSHClient, sftp: paramiko.SFTPClient,
                 trans: paramiko.Transport, role: str):
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.ssh = ssh
        self.sftp = sftp
        self.trans = trans
        self.role = role

    def get_host(self):
        return self.host

    def get_port(self):
        return self.port

    def get_username(self):
        return self.username

    def get_password(self):
        return self.password

    def get_ssh(self):
        return self.ssh

    def get_sftp(self):
        return self.sftp

    def get_trans(self):
        return self.trans

    def free_conn(self):
        # ssh
        if self.ssh != None:
            self.ssh.close()
            self.ssh = None

        # sftp
        if self.trans != None:
            self.trans.close()
            self.trans = None
            self.sftp = None

    def get_role(self):
        return self.role


class InstanceConnectionK8S:

    def __init__(self, name: str, namespace: str, role: str):
        self.podname = name
        self.namespace = namespace
        self.role = role

    def get_podname(self):
        return self.podname

    def get_namespace(self):
        return self.namespace

    def get_role(self):
        return self.role


class InstanceConnection:

    def __init__(self, machine: InstanceConnectionMachine,
                 k8s: InstanceConnectionK8S):
        self.machine = machine
        self.k8s = k8s

    def get_machine(self):
        return self.machine

    def get_k8s(self):
        return self.k8s

    def free_conn(self):
        if self.machine != None:
            self.machine.free_conn()
        return None


class InstanceConnections:

    def __init__(self):
        self.conns: InstanceConnection = []
        self.number = 0

    def add(self, instance: InstanceConnection):
        self.conns.append(instance)
        self.number = len(self.conns)

    def get_conns(self):
        return self.conns

    def get_number(self):
        return self.number

    def free_conns(self):
        for conn in self.conns:
            conn.free_conn()
