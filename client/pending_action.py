class PendingAction():
    
    def __init__(self, message, username):
        self.__action = 'send' if message.sender == username else 'recv'
        self.__sender = message.sender
        self.__receiver = message.receiver
        self.__message_text = message.text

    @property
    def action(self):
        return self.__action

    @property
    def sender(self):
        return self.__sender

    @property
    def receiver(self):
        return self.__receiver

    @property
    def message_text(self):
        return self.__message_text

    def __str__(self):
        return str({'action' : self.__action, 'sender': self.__sender, 'dest': self.__receiver, 'message': self.__message_text})

    def __repr__(self):
        return self.__str__()
