from __future__ import print_function

# Prints to stdout the CPU time (system + user space) consumed by the
# process with the given PID in milliseconds.
#
# python cputime.py <pid>
#

import os
import sys

clk_tck = os.sysconf(os.sysconf_names['SC_CLK_TCK'])

def get_time_millis(pid):
    with open('/proc/%d/stat'%pid, 'r') as stat_file:
        line = stat_file.readline()
        data = line.split(' ')
        cutime = int(data[13])
        cstime = int(data[14])
    total_jiffies = cutime + cstime
    total_millis = 1000 * total_jiffies // clk_tck
    return total_millis

def get_time_seconds(pid):
    total_millis = get_time_millis(pid)
    return float(total_millis) / 1000

def main():
    if len(sys.argv) < 2:
        print('Error: process id expected', file=sys.stderr)
    pid = int(sys.argv[1])
    total_millis = get_time_millis(pid)
    print(total_millis)

if __name__ == "__main__":
    main()
