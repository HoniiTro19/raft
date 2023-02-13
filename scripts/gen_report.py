#!/usr/bin/env python3
import os
import argparse
import json
import paramiko
from pathlib import Path

# def generate_single(log):
#     with open(log) as f:
    
class RemoteServer:
    def __init__(self, addr, file, prkey='~/.ssh/id_rsa'):    
        addrs = addr.split('@')
        self.user = addrs[0]
        self.host = addrs[1]
        self.file = file
        self.prkey = prkey
        
    def __iter__(self):
        cmd = 'cat ' + self.file
        _, stdout, stderr = self.server.exec_command(cmd)
        self.result = 
        
    def __next__(self):
        
    def start_server(self):
        self.server = paramiko.SSHClient()
        self.server.load_system_host_keys()
        self.server.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        privatekeyfile = os.path.expanduser(self.prkey)
        mykey = paramiko.RSAKey.from_private_key_file(privatekeyfile)
        self.server.connect(self.host, 22, self.user, pkey=mykey, timeout=5)
    
if __name__ == "__main__":
    description = 'generate reports for single or several election experiments'
    parser = argparse.ArgumentParser(description)
    parser.add_argument('type', choices=['single', 'group'], 
                        help='generate statistics or graphic reports for the experiments')
    parser.add_argument('mode', choices=['normal', 'mttr'], 
                        help='normal long running experiments or downtime experiments to test mttr(mean time to repair)')
    parser.add_argument('dir', type=Path, help='the result directory to generate reports')
    parser.add_argument('peers', help='the comma-separated and ordered ssh addresses of raft instances')
    args = parser.parse_args()
    peers = args.peers.split(',')
