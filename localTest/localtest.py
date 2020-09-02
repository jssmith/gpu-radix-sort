import subprocess

def lambda_handler(event, context):
    res = subprocess.run("/var/task/radix_sort", capture_output=True)
    return {
        "success": res.returncode == 0,
        "stdout": res.stdout,
        "stderr": res.stderr
    }   
    
if __name__ == "__main__":
    print(lambda_handler({}, None))

