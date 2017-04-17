def report_loglines(loglines, from_, to_=None, reason=None, blanks=0, printend=False):
    if to_ is None:
        to_ = from_
    from_t = from_ - 3
    to_t = to_ + 7

    blanks_str = " "*blanks

    print blanks_str + "-------- thread --------"
    if reason:
        print(blanks_str + "reason: %s" % reason)

    if from_t < 0:
        print blanks_str + "  <start>"
        from_t = 0
    else:
        print blanks_str + "  ...more..."

    for i in range(from_t, from_):
        print blanks_str + "  %s" % loglines[i]
    for i in range(from_, to_):
        print blanks_str + "| %s" % loglines[i]
    print blanks_str + "> %s" % loglines[to_]
    for i in range(to_+1, min(to_t, len(loglines))):
        print blanks_str + "  %s" % loglines[i]
    if to_t >= len(loglines):
        print blanks_str + "  <end>"
    else:
        print blanks_str + "  ...more..."

    if printend:
        print blanks_str + "-------- end -----------"
