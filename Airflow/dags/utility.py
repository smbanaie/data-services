class TopicEmptyError(Exception):
    def __init__(self, message, topic):            
        # Call the base class constructor with the parameters it needs
        super().__init__(message)
            
        # Now for your custom code...
        self.topic = topic