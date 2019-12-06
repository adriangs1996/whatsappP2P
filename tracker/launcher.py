import argparse
import re
from tracker import ClientInformationTracker


def main():
    argsparser = argparse.ArgumentParser()
    argsparser.add_argument('-i', help='ip address for this node')
    argsparser.add_argument('-p', type=int, help='port for this node')
    argsparser.add_argument('-t',
                            help='list of kwnow chord peers',
                            type=str,
                            nargs='+'
                            )
    arguments = argsparser.parse_args()
    ip, port, bootstrap_nodes = arguments.i, arguments.p, arguments.t
    if None in (ip, port, bootstrap_nodes):
        argsparser.print_help()
        exit(0)
    nodes = []
    # Prefer this way to list comprehesion for readability
    for peer in bootstrap_nodes:
        peer = peer.split(":")
        nodes.append((peer[0], int(peer[1])))
    bootstrap_nodes = nodes
    print(bootstrap_nodes)

    trac = ClientInformationTracker(ip, port, bootstrap_nodes)
    trac.start_services()


if __name__ == '__main__':
    main()
