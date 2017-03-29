def report_loglines(loglines, from_, to_=None, reason=None):
    if to_ is None:
        to_ = from_
    from_t = from_ - 3
    to_t = to_ + 7

    print "-------- LogLines ------"
    if reason:
        print("reason: %s" % reason)
    if from_t < 0:
        print "  <start>"
        from_t = 0
    else:
        print "  ...more..."
    for i in range(from_t, from_):
        print "  %s" % loglines[i]
    for i in range(from_, to_-1):
        print "| %s" % loglines[i]
    print "> %s" % loglines[to_]
    for i in range(to_+1, min(to_t, len(loglines))):
        print "  %s" % loglines[i]
    if to_t >= len(loglines):
        print "  <end>"
    else:
        print "  ...more..."
