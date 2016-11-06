#!/usr/bin/python

import datetime
import getopt
import os
import subprocess
import sys


def getCluster(executable, cluster_file):
    if len(cluster_file) == 0:
        cluster_file = os.path.join(os.path.dirname(executable), "hosts")
    f = open(cluster_file)
    result = [ host.strip() for host in f.readlines() ]
    f.close()
    return result


def ssh(host, cmd):
    user = "root"
    ssh = subprocess.Popen(["ssh", "%s@%s" % (user, host), cmd],
                           shell=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    return [ssh.stdout.readlines(), ssh.stderr.readlines()]


def scp(host, remote_dir, local_dir):
    user = "root"
    scp = subprocess.Popen(["scp", "-r", "%s@%s:%s" % (user, host, remote_dir), local_dir],
                           shell=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    return [scp.stdout.readlines(), scp.stderr.readlines()]


def local_addr(host):
    result = ssh(host, "ifconfig")
    out = result[0]
    for i in range(0, len(out)):
        if out[i].startswith("eth0:"):
            return out[i + 1].split()[1]


def cmd_cluster(cluster):
    for host in cluster:
        print(host)


def cmd_get_logs(cluster, remote_log_dir):
    print("cmd_get_logs(%s, %s) is untested - aborting" % (cluster, remote_log_dir))
    sys.exit(2)

    dt = datetime.datetime.now()
    dir = "{:04d}{:02d}{:02d}-{:02d}{:02d}{:02d}".format(dt.year, dt.month, dt.day, dt.hour, dt.minute, dt.second)
    print(dir)
    if os.path.isdir(dir):
        print("'%s' exists - aborting" % dir)
        sys.exit(2)
    os.makedirs(dir)
    basename = os.path.basename(remote_log_dir)
    for host in cluster:
        scp(host, remote_log_dir, dir)
        os.rename(os.path.join(dir, basename), os.path.join(dir, host))


def cmd_local_addr(cluster):
    for host in cluster:
        print(local_addr(host))


def cmd_ssh(cluster, cmd):
    for host in cluster:
        print(">>> %s <<<" % host)
        for line in ssh(host, cmd)[0]:
            print(line.rstrip())


def print_help(executable):
    print(executable + " command [-c <cluster_file>]")
    print("available commands are:")
    indent = ' ' * 4
    print(indent + "cluster")
    print(indent + "get_logs <arg>")
    print(indent + "local_addr")
    print(indent + "ssh <arg>")


def get_global_params(argv, executable):
    params = {}
    try:
        opts, args = getopt.getopt(argv, "hc:", ["cluster="])
    except getopt.GetoptError:
        print_help(executable)
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print_help(executable)
            sys.exit()
        elif opt in ("-c", "--cluster"):
            params["cluster_file"] = arg
    return params, args


def main(argv):
    executable = argv[0]

    params, args = get_global_params(argv[1:], executable)

    if len(args) < 1:
        print_help(executable)
        sys.exit(2)
    cmd = args[0]

    cluster = getCluster(executable, params.get("cluster_file", ""))

    if cmd == "cluster":
        cmd_cluster(cluster)
    elif cmd == "get_logs" and len(args) > 1:
        cmd_get_logs(cluster, args[1])
    elif cmd == "local_addr":
        cmd_local_addr(cluster)
    elif cmd == "ssh" and len(args) > 1:
        cmd_ssh(cluster, args[1])
    else:
        print_help(executable)
        sys.exit(2)

if __name__ == "__main__":
    #print(os.getenv("SSH_AUTH_SOCK"))
    main(sys.argv)
