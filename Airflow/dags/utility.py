class TopicEmptyError(Exception):
    def __init__(self, message, topic):            
        # Call the base class constructor with the parameters it needs
        super().__init__(message)
            
        # Now for your custom code...
        self.topic = topic


class KafkaSchemaNotFound(Exception):
    def __init__(self, message, schema_name):            
        # Call the base class constructor with the parameters it needs
        super().__init__(message)
            
        # Now for your custom code...
        self.schema = schema_name

class NumberOfAffectedRowsMismatch(Exception):
    def __init__(self, message, etl):            
        # Call the base class constructor with the parameters it needs
        super().__init__(message)
            
        # Now for your custom code...
        self.etl = etl