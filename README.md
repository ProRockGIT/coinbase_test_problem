# coinbase_test_problem
To collect tades data run main.py with flag -c.
To test volume validity run main.py -t <time> where <time> is "local" to calculate cumulative volume for last hour using local time or "coinapi" to use coinapi time.
To inject faults use -f <skips>: int, forces collecting script to skip given number of steps, can stack.
