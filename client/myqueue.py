class Queue:
    """Defines queue-like behavior type"""

    def __init__(self, capacity=50, auto_growth=False, items=[]):
        """Initializes a new Queue instance.
        
        >- capacity (int): maximum of elements to store [Optional].
        >- auto_growth (bool): indicates if the queue can increase its capacity when needed [Optional].
        >- items (list): list of elements to add to the queue in order of addition (from first to last). If provided the capacity will be max(capacity, len(items)) [Optional].
        """
                  
        self.__auto_growth = auto_growth                                    # queue can increase capacity when needed
        self.__count = len(items)                                           # actual valid elements in queue
        self.__capacity = max(capacity, self.__count)                       # maximum of elements to store
        self.__head = 0                                                     # start index of the queue: the head (first element added)
        self.__tail = self.__count % self.__capacity                        # end index of the queue: the tail (latest element added)
        
        self.__elements = items                                             # list of elements (len = capacity)
        self.__elements.extend([None] * (self.__capacity - self.__count))
            

    def peek(self):
        """Gets the element at the top of the queue."""

        if self.__count == 0:
            raise RuntimeError("Queue is empty")

        return self.__elements[self.__head]

    def pop(self):
        """Gets and removes the element at the top of the queue."""

        element = self.peek()

        self.__count -= 1
        self.__elements[self.__head] = None
        self.__head = (self.__head + 1) % self.__capacity
        
        return element
        
    def enqueue(self, element):
        """Adds element to the end of the queue
        
        >- element: element to enqueue
        """

        if self.isFull:
            if not self.__auto_growth:
                raise RuntimeError("Queue is full")
            
            self.__grow()
            
        self.__elements[self.__tail] = element
        self.__tail = (self.__tail + 1) % self.__capacity
        self.__count += 1

    def smart_enqueue(self, element):
        try:
            self.enqueue(element)
        except RuntimeError as error:
            self.pop()
            self.enqueue(element)

    def __getitem__(self, index):
        if index >= self.__count:
            raise IndexError

        return self.__elements[(self.__head + index) % self.__capacity]

    def __len__(self):
        return self.__count

    def __str__(self):
        return str(self.items)
         
    def __repr__(self):
        return self.__str__()

    def __bool__(self):
        return not self.isEmpty

    def __contains__(self, value):
        return value in self.items
            
    def __grow(self):
        """Increses the capacity of the queue."""
        self.__elements = self.items + ([None] * self.__capacity)
        self.__head = 0
        self.__tail = self.__count
        self.__capacity = len(self.__elements)

    @property
    def isEmpty(self):
        """Indicates if the queue is empty: count == 0."""
        return self.__count == 0

    @property
    def isFull(self):
        """Indicates if the queue is full: has -capacity- elements."""
        return self.__count == self.__capacity

    @property
    def capacity(self):
        """Total of elements the queue is capable of hold."""
        return self.__capacity

    @property
    def count(self):
        """Number of elements in queue."""
        return self.__count

    @property
    def items(self):
        """Return the elements in the queue in order of addition (first to last)."""
        if self.__count == 0:
            return []
        if self.__head < self.__tail:
            return self.__elements[self.__head:self.__tail]
        return self.__elements[self.__head:] + self.__elements[:self.__tail]
