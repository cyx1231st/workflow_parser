COMPONENT = "component"
TARGET = "target"
HOST = "host"
THREAD = "thread"
REQUEST = "request"
KEYWORD = "keyword"
TIME = "time"
SECONDS = "seconds"

ALL_VARS = {COMPONENT, TARGET, HOST, THREAD, REQUEST, KEYWORD, TIME, SECONDS}
TARGET_VARS = {COMPONENT, TARGET, HOST}
LINE_VARS = ALL_VARS - TARGET_VARS
