import chord as chord
import argparse
import re


def main():
    argsparser = argparse.ArgumentParser()
    argsparser.add_argument('-i', help='ip address for this node')
    argsparser.add_argument('-p', type=int, help='port for this node')
    argsparser.add_argument('-t',
                            help='url of a known peer of a CHORD ring\
                                 to join this node to'
                            )

    args = argsparser.parse_args()
    ip, port, target_url = args.i, args.p, args.t
    if None in (ip, port):
        print("Suply ip and port for node")
        exit(0)
    if target_url is None:
        node = chord.Node(ip, port)
    else:
        target_url = target_url.split(':')
        node = chord.Node(ip, port, (target_url[0], int(target_url[1])))
    node.start_service()


if __name__ == '__main__':
    main()
