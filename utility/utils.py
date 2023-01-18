import subprocess
import pickle
import random
import string

def get_time() -> int:
    return int(subprocess.check_output(["date", "+\%s"])[1:])

def get_hostname() -> str:
    return subprocess.check_output(["hostname"]).decode('ascii')[:-1]

def serialize(obj) -> bytes:
    return pickle.dumps(obj)

def deserialize(bytes):
    return pickle.loads(bytes)

def get_rand_str(length=8) -> str:
    # choose from all lowercase letter
    letters = string.ascii_lowercase
    rand_str = ''.join(random.choice(letters) for _ in range(length))
    return rand_str
