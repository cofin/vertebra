import os
import sys
import time

def main():
    print("Worker started!")
    notify_fd = os.environ.get("VERTEBRA_NOTIFY_FD")
    req_fd = os.environ.get("VERTEBRA_REQ_SHM_FD")
    resp_fd = os.environ.get("VERTEBRA_RESP_SHM_FD")
    
    print(f"FDs: notify={notify_fd}, req={req_fd}, resp={resp_fd}")
    
    # Write to verification file
    with open("/tmp/vertebra_worker_test.txt", "w") as f:
        f.write(f"notify={notify_fd}\nreq={req_fd}\nresp={resp_fd}\n")
    
    # Keep alive for a bit
    time.sleep(2)

if __name__ == "__main__":
    main()

