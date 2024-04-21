class LockReleasedError(Exception):
    """
    Exception raised when lock is released.
    """
    
class MaxRetriesExceededOnXReadError(Exception):
    """
    Exception raised when max retries exceeded on xread.
    """