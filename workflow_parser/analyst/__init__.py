from __future__ import print_function

from collections import OrderedDict


class Report(object):
    def __init__(self, name):
        self.outfile = None
        self.contents = []
        self.key_len = 0
        self.name = name
        self.register("Name", name)
        self.content_dict = OrderedDict()

    def __getitem__(self, item):
        return self.content_dict[item]

    def __setitem__(self, item, val):
        return self.register(item, val)

    def register(self, key, value):
        if key in self.content_dict.keys():
            raise RuntimeError("Report %s already has key %s!"
                               % (self.name, key))
        self.content_dict[key] = value
        self.contents.append((key, value))
        self.key_len = max(self.key_len, len(key))

    def blank(self):
        self.contents.append(None)

    def set_outfile(self, outfile, print_header):
        self.outfile = outfile
        self.print_header = print_header

    def export_terminal(self):
        print("\n >> FINAL REPORT(%s):" % self.name)
        for content in self.contents:
            if content is None:
                print("")
            elif isinstance(content[1], Integral):
                format_str = "{:<" + str(self.key_len + 2) + "}{:d}"
                print(format_str.format(content[0]+":", content[1]))
            elif isinstance(content[1], Real):
                format_str = "{:<" + str(self.key_len + 2) + "}{:7.5f}"
                print(format_str.format(content[0]+":", content[1]))
            else:
                format_str = "{:<" + str(self.key_len + 2) + "}{:s}"
                print(format_str.format(content[0]+":", content[1]))


    def export(self):
        if self.outfile is None:
            self.export_terminal()
        else:
            outfile = open(self.outfile, "a+")
            if self.print_header:
                header_fields = [content[0] for content in self.contents
                                 if content is not None]
                outfile.write(",".join(header_fields) + '\n')
            row_fields = [str(content[1]) for content in self.contents
                          if content is not None]
            outfile.write(",".join(row_fields) + '\n')
            outfile.flush()
            outfile.close()
