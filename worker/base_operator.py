import signal
import sys

class BaseOperator:
    """Abstract base class for all task operators.

    Subclasses must implement initialize(), run(), and finish().
    The finish() method is guaranteed to run even on SIGTERM (cancellation).
    """
    def initialize(self, payload : dict , connection: dict):
        """setup -  run before work starts"""
        raise NotImplementedError
    
    def run(self):
        """the actual work runs here"""
        raise NotImplementedError
    
    def finish(self):
        """cleanup - always runs at the end, even on cancel"""
        raise NotImplementedError
    
def run_operator(operator: BaseOperator, payload : dict , connection : dict):
    """
    This is standard entrypoint every operator script calls.
    Handles SIGTERM (cancel) as - whenever SIGTERM arrives, call handle_sigterm(clean up) instead of dying immediately.
    """
    
    def handle_sigterm(signum, frame):
        """Handle SIGTERM by running finish() for graceful cleanup before exit."""
        print("[Operator] SIGTERM received. Running finish() and exiting.")
        try:
            operator.finish()
        except Exception as e:
            print(f"[Operator] Error in finish() : {e}")
        sys.exit(0)
        
    signal.signal(signal.SIGTERM, handle_sigterm)
    
    operator.initialize(payload, connection)
    operator.run()
    operator.finish()