import time
import atexit
from current import Current


def show_duration(fun):
    def time_wrap():
        start = time.time()
        fun()
        duration = int((time.time() - start) / 60)
        unit = 'Minutes'
        if duration > 60:
            duration = round((duration/60), 2)
            unit = 'Hours'
        print('\n\nCompleted process in {} {}'.format(duration, unit))
    return time_wrap()

def handle_current(fun):
    def update_current():
        fun()
        curr = Current()
        atexit.register(curr.update)
        print('Updated Current')
    return update_current()
