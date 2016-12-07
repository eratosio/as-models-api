
class _Sentinel(object):
	def __eq__(self, other):
		return self is other

SENTINEL = _Sentinel()
