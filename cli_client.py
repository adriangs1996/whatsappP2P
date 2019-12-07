from random import randrange
from tracker.tracker import request_tracker_action
from hashlib import sha1
TRACKIP = ("127.0.0.1", 8888)
MAXKEY = 1<<160
REGISTER = "register_client"
GET = "locate"
IPS = ["192.168.1.1", "192.168.1.2", "192.168.1.3"]

if __name__ == "__main__":
    # Do a CLI Loop
    generated_keys = []
    while True:
        # Catch Command
        command = input(">")
        if command == "generate":
            key = randrange(MAXKEY)
            generated_keys.append(key)
            print(f"Generated {key}")
            command = input("Enter what to do >")
            if command == "put":
                response = request_tracker_action(
                    TRACKIP[0],
                    TRACKIP[1],
                    REGISTER,
                    user=key,
                    ip=IPS[randrange(3)],
                    port=randrange(6000, 8000)
                )
                print("Succesfully added key")
            elif command == "get":
                response = request_tracker_action(
                    TRACKIP[0],
                    TRACKIP[1],
                    GET,
                    user=key
                )
                print(response)
            else:
                continue
        elif command == "list":
            for i, key in enumerate(generated_keys, 0):
                print(f"{i} --> {key}")
        elif command == "skey":
            i = int(input("which key"))
            key = generated_keys[i]
            command == input("select what to do >")
            if command == "put":
                response = request_tracker_action(
                    TRACKIP[0],
                    TRACKIP[1],
                    REGISTER,
                    user=key,
                    ip=IPS[randrange(3)],
                    port=randrange(6000, 8000)
                )
                print("Succesfully added key")
            elif command == "get":
                response = request_tracker_action(
                    TRACKIP[0],
                    TRACKIP[1],
                    GET,
                    user=key
                )
                print(response)
            else:
                continue
        else:
            print("invalid Command")
