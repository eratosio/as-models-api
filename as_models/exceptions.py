# -*- coding: UTF-8 -*-


class SenapsError(Exception):
    """
    Senaps abstract base class.
    """
    pass


class SenapsModelError(SenapsError):

    def __init__(self, msg, user_data=None):
        """
        Custom error intended to be raised by client programmers.
        :param msg: str: human readable message.
        :param user_data: json-encodable python-dict.
        """
        self.msg = msg
        self.user_data = user_data

    def __str__(self):
        """
        Get a human readable version of the error. Note: will NOT report the user_data field,
        since it could be very large and we do not accept that in tracebacks.
        :return: str
        """
        return '{0}\nuser_data:{1}\n'.format(self.msg,
                                             str(self.user_data) if self.user_data is None else '<embedded>')
