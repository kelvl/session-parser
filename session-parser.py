import csv
import sys
from collections import namedtuple

SESSION_THRESHOLD_DURATION_SECONDS = 300

state = {
    "ts_open": None,
    "previous_row": None
}

Row = namedtuple('Row', 'ts, user, ip, day_of_month, app, host, date')

fout = open(sys.argv[2], "w")

def write_out(row, event, duration):
    # if row.app != "unknown":
    # print "\t".join([str(x) for x in row]), "\t", event, "\t", duration
    fout.write("\t".join([str(x) for x in row]) + "\t" + event + "\t" + str(duration) + "\n")

def close_last():
    write_out(state['previous_row'], "close", state['previous_row'].ts - state['ts_open'])
    return

def parse_row(row):
    if state['previous_row'] is None:
        write_out(row, "open", 0)
        state['previous_row'] = row
        state['ts_open'] = row.ts
        return

    if state['previous_row'].user != row.user:
        write_out(state['previous_row'], 'close', state['previous_row'].ts - state['ts_open'])
        write_out(row, 'open', 0)
        state['ts_open'] = row.ts
        state['previous_row'] = row
        return

    if state['previous_row'].app != row.app:
        if (row.ts - state['previous_row'].ts) > SESSION_THRESHOLD_DURATION_SECONDS:
            prev = state['previous_row']
            prepared_row = Row(prev.ts + SESSION_THRESHOLD_DURATION_SECONDS, prev.user, prev.ip, prev.day_of_month, prev.app, prev.host, prev.date)
            write_out(prepared_row, 'close', SESSION_THRESHOLD_DURATION_SECONDS)
        else:
            prev = state['previous_row']
            prepared_row = Row(row.ts, prev.user, prev.ip, prev.day_of_month, prev.app, prev.host, prev.date)
            write_out(prepared_row, 'close', row.ts - state['ts_open'])
            
        write_out(row, 'open', 0)
        state['ts_open'] = row.ts
        state['previous_row'] = row
        return

    write_out(row, 'running', 0)
    state['previous_row'] = row
    return


with open(sys.argv[1], "r") as f:
    rows = csv.reader(f, delimiter="\t")
    for i in rows:
        r = Row._make([float(i[0])] + i[1:])
        parse_row(r)
    close_last()

fout.close()

