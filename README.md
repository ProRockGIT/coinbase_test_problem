# coinbase_test_problem
To collect tades data run main.py -c.

To test volume validity run main.py -t `time`: str where `time` is "local" to calculate cumulative volume for last hour using local time or "coinapi" to use coinapi time.
  
To inject faults run main.py -f `skips`: int, forces collecting script to skip given number of steps, can stack.
