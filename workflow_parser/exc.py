class WFException(Exception):
    def __init__(self, msg, e=None):
        assert isinstance(msg, str)
        if e:
            assert isinstance(e, Exception)

        self.e = e
        super(WFException, self).__init__(msg)

    def __str__(self):
        return self._to_str(0)

    def _to_str(self, indent):
        ret = "\n%s%s" % ("> "*indent, self.message)
        if self.e:
            if isinstance(self.e, WFException):
                ret += self.e._to_str(indent+1)
            else:
                ret += "\n%s%r" % ("> "*(indent+1), self.e)
        return ret
