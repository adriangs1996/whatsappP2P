import argparse
from tracker.tracker import request_tracker_action

argumentParser = argparse.ArgumentParser()
argumentParser.add_argument('-host', type=str, help="Tracker to connect to")

arguments = argumentParser.parse_args()

tracker = arguments.host
tracker = tracker.split(":")
tracker_ip, tracker_port = tracker[0], int(tracker[1])

while 1:
    cmd = input("enter a command >")
    action = cmd[0]
    cmd = cmd.split()[1:]
    if len(cmd) == 1:
        response = request_tracker_action(tracker_ip, tracker_port, action, user=cmd[0])
    elif len(cmd) == 3:
        response = request_tracker_action(tracker_ip, tracker_port, action, user=cmd[0], ip=cmd[1], port=int(cmd[2]))
    print(response)