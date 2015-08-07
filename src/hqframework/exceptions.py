class LaunchTaskException(Exception):
    def __init__(self, message):
        super(LaunchTaskException, self).__init__(message)


class GetWorkersException(Exception):
    def __init__(self, message):
        super(GetWorkersException, self).__init__(message)
