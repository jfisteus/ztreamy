import time
import numpy
import math

import streamsem
from streamsem import StreamsemException

def exponential_event_scheduler(mean_time):
    last = time.time()
    while True:
        last += numpy.random.exponential(mean_time)
        yield last

def constant_event_scheduler(mean_time):
    last = time.time()
    while True:
        last += mean_time
        yield last

def get_scheduler(description):
    pos = description.find('[')
    if pos == -1 or description[-1] != ']':
        raise StreamsemException('error in distribution specification',
                                 'event_source params')
    distribution = description[:pos].strip()
    params = [float(num) for num in description[pos + 1:-1].split(',')]
    if distribution == 'exp':
        if len(params) != 1:
            raise StreamsemException('exp distribution needs 1 param',
                                     'event_source params')
        return exponential_event_scheduler(params[0])
    elif distribution == 'const':
        if len(params) != 1:
            raise StreamsemException('const distribution needs 1 param',
                                     'event_source params')
        return constant_event_scheduler(params[0])

def median(data):
    """Returns the statistic median of a list of values."""
    data = sorted(data)
    n = len(data)
    if n % 2 == 0:
        return float(data[n // 2 - 1] + data[n // 2]) / 2
    else:
        return float(data[n // 2])

def average_and_std_dev(data):
    """Returns the average and sample standard deviation of data."""
    n = len(data)
    total = sum(data)
    average = float(total) / n
    std_dev = math.sqrt((sum([(d - average) * (d - average) for d in data])
                         / (n - 1)))
    return average, std_dev
