from random import randrange
from tracker import request_tracker_action
from hashlib import sha1
TRACKIP = ("127.0.0.1", 8888)
MAXKEY = 1<<160
REGISTER = "register_client"
GET = "locate"
IPS = ["192.168.1.1", "192.168.1.2", "192.168.1.3"]


def fuzz_name(size):
    alphabet = [chr(char) for char in range(ord('a'), ord('z') + 1)]
    string = ""
    for _ in range(size):
        string += alphabet[randrange(len(alphabet))]
    return string


if __name__ == "__main__":
    # Do a CLI Loop
    generated_keys = []
    while True:
        # Catch Command
        command = input(">")
        if command == "generate":
            key = fuzz_name(20)
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
                if response:
                    print("Succesfully added key")
                else:
                    print("Key already aded to DHT")
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
            i = int(input("which key: "))
            key = generated_keys[i]
            print("selected key %s" % key)
            command = input("select what to do >")
            print(f"doing {command}")
            if command == "put":
                response = request_tracker_action(
                    TRACKIP[0],
                    TRACKIP[1],
                    REGISTER,
                    user=key,
                    ip=IPS[randrange(3)],
                    port=randrange(6000, 8000)
                )
                if response:
                    print("Succesfully added key")
                else:
                    print("Key already in DHT")
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
