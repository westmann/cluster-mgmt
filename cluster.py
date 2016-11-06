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
    result = [host.strip() for host in f.readlines()]
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


def cmd_cluster_config(cluster, cluster_params):
    hosts = [local_addr(host) for host in cluster]
    # hosts = ["10.0.242.20", "10.0.182.131", "10.0.154.94"]
    ncs_per_host = cluster_params.get("ncs", 2)
    part_per_nc = cluster_params.get("partitions", 2)
    no_hosts = cluster_params.get("hosts", len(hosts))

    conf_name = "h{:d}n{:d}p{:d}".format(no_hosts, ncs_per_host, part_per_nc)

    for h in range(0, no_hosts):
        for n in range(0, ncs_per_host):
            nc_id = "{:s}{:d}".format(chr(ord('a') + h), n)
            print("[nc/{:s}]".format(nc_id))
            print("address={:s}".format(hosts[h]))
            print("port={:d}".format(9090 + n))
            dir = "/cbas/{:s}/{:s}/analytics".format(conf_name, nc_id)
            print("txnlogdir={:s}/log/txnlog".format(dir))
            print("coredumpdir={:s}/log/coredump".format(dir))
            io_dirs = ["{:s}/io/{:d}".format(dir, i) for i in range(0, part_per_nc)]
            print("iodevices={:s}\n".format(",".join(io_dirs)))


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
    message = """usage:
    {:s} [-c<cluster_file>] command

available commands are:
    cluster
    cluster_config [-h<no_hosts>] [-p<ncs_per_host>] [-p<partitions_per_nc>]
    get_logs <remote_log_dir>
    local_addr
    ssh <remote_cmd>
    """.format(executable)
    print(message)


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


def get_cluster_params(argv, executable):
    params = {}
    try:
        opts, args = getopt.getopt(argv, "h:n:p:", ["hosts=", "ncs=", "partitions="])
    except getopt.GetoptError:
        print_help(executable)
        sys.exit(2)
    for opt, arg in opts:
        if opt in ("-h", "--hosts"):
            params["hosts"] = int(arg)
        elif opt in ("-n", "--ncs"):
            params["ncs"] = int(arg)
        elif opt in ("-p", "--partitions"):
            params["partitions"] = int(arg)
    if len(args) > 0:
        print("unused cluster parameters {:s}", args)
        sys.exit(2)
    return params, args


def main(argv):
    executable = argv[0]

    params, args = get_global_params(argv[1:], executable)

    if len(args) < 1:
        print_help(executable)
        sys.exit(2)
    cmd = args[0]
    args = args[1:]

    cluster = getCluster(executable, params.get("cluster_file", ""))

    if cmd == "cluster":
        cmd_cluster(cluster)
    elif cmd == "cluster_config":
        cluster_params, args = get_cluster_params(args, executable)
        cmd_cluster_config(cluster, cluster_params)
    elif cmd == "get_logs" and len(args) > 0:
        cmd_get_logs(cluster, args[0])
    elif cmd == "local_addr":
        cmd_local_addr(cluster)
    elif cmd == "ssh" and len(args) > 0:
        cmd_ssh(cluster, args[0])
    else:
        print_help(executable)
        sys.exit(2)


if __name__ == "__main__":
    # print(os.getenv("SSH_AUTH_SOCK"))
    main(sys.argv)
