from __future__ import print_function

import atexit
from os import path
from json import dumps, loads


class Current:
    def __init__(self):
        self.current_seed = {'counter': 0}
        self.current = self.read_current()

    def read_current(self):
        return loads(open("current.json", "r").read()) if path.exists("current.json") else self.current_seed

    def get_counter(self):
        return int(self.current['counter']) + 1

    def update(self):
        with open("current.json", "w") as f:
            f.write(dumps(self.current))


def main():
    print("I have been run {} times".format(counter))


if __name__ == "__main__":
    main()