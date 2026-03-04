import sys
import os
import json

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from base_operator import BaseOperator, run_operator

class ExampleOperator(BaseOperator):
    def initialize(self, payload : dict, connection: dict):
        self.task_name = payload.get("task_name", "unknown")
        print(f"[ExampleOperator] initialize() called for '{self.task_name}'")
        
    def run(self):
        print(f"[ExampleOperator] run() started for '{self.task_name}'")
        import time
        for i in range(10):
            print(f"[ExampleOperator] working... step {i+1}/10")
            time.sleep(2)
        print(f"[ExampleOperator] run() completed for '{self.task_name}'")
        
    def finish(self):
        print(f"[ExampleOperator] finish() called. Cleaning up '{self.task_name}'")

if __name__ == "__main__":
    payload = json.loads(sys.argv[1])
    connection = json.loads(sys.argv[2])
    run_operator(ExampleOperator(), payload , connection)
    