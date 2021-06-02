"""
Code that will not produce a greetings.txt file which will raise an error in
the analytics step.
"""

import argparse
import sys


if __name__ == '__main__':
    args = sys.argv[1:]

    parser = argparse.ArgumentParser()
    parser.add_argument("-i", "--inputfile", required=True)
    parser.add_argument("-o", "--outputfile", required=True)
    parser.add_argument("-g", "--greeting", default='Hello', required=False)
    parser.add_argument("-s", "--sleeptime", default=1.0, type=float, required=False)

    parsed_args = parser.parse_args(args)

    print('This is the error test case.')
