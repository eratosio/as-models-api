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
        return '{0}\nuser_data:{1}\n'.format(self.msg,
                                             str(self.user_data))
