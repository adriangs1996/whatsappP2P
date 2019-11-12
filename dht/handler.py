import chord
import argparse
import re

def main():
    url_regex = re.compile(r'(?P<host>[A-Za-z]+):(?P<port>[1-9][0-9]{3,4})')
    argsparser = argparse.ArgumentParser()
    argsparser.add_argument('-i', help='ip address for this node')
    argsparser.add_argument('-p', type=int, help='port for this node')
    argsparser.add_argument('-t', help='url of a known peer of a CHORD ring to join this node to')

    args = argsparser.parse_args()
    ip, port, target_url = args.i, args.p, args.t
    if None in (ip, port):
        print("Suply ip and port for node")
        exit(0)
    if target_url is None:
        node = chord.Node(ip, port)
    else:
        url_dict = url_regex.match(target_url).groupdict()
        node = chord.Node(ip, port, (url_dict['host'], int(url_dict['port'])))

if __name__ == '__main__':
    main()
