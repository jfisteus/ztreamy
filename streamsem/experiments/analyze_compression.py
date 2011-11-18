from optparse import OptionParser

def compression_ratio(filename):
    comp_size = 0
    raw_size = 0
    ratios = []
    with open(filename, 'r') as file_:
        for line in file_:
            if line.startswith('#'):
                continue
            data = [s.strip() for s in line.split('\t')]
            if data[0] == 'data_receive':
                comp_size += int(data[1])
                raw_size += int(data[2])
                if raw_size > 0:
                    ratios.append((raw_size, float(comp_size) / raw_size))
    return ratios

def read_cmd_options():
    parser = OptionParser(usage = 'usage: %prog [options] <log_filename>')
    (options, args) = parser.parse_args()
    if len(args) == 1:
        options.filename = args[0]
    else:
        parser.error('Log filename expected')
    return options

def main():
    options = read_cmd_options()
    data = compression_ratio(options.filename)
    print '\n'.join(['%d\t%.6f'%(t, r) for t, r in data])

if __name__ == '__main__':
    main()
