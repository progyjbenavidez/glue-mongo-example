

import sys
import boto3
import paramiko
 
s3 = boto3.resource("s3")
bucket = s3.Bucket(name="destination-bucket")
bucket.load()
 
"""
JOB PARAMATERS 
KEY: --additional-python-modules
VALUE: paramiko==3.1.0
--------------------
KEY: --pip-install
VALUE: paramiko==3.1.0
------------------
"""
def main():
    ssh = paramiko.SSHClient()
    # In prod, add explicitly the rsa key of the host instead of using the AutoAddPolicy:
    # ssh.get_host_keys().add('example.com', 'ssh-rsa', paramiko.RSAKey(data=decodebytes(b"""AAAAB3NzaC1yc2EAAAABIwAAAQEA0hV...""")))
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    
    ssh.connect(
        hostname="sftp.example.com",
        username="thisdataguy",
        password="very secret",
    )
    sftp = ssh.open_sftp()
    for filename in sftp.listdir():
        print(f"Downloading {filename} from sftp...")
        # mode: ssh treats all files as binary anyway, to 'b' is ignored.
        with sftp.file(filename, mode="r") as file_obj:
            print(f"uploading  {filename} to s3...")
            bucket.put_object(Body=file_obj, Key=f"destdir/{filename}")
            print(f"All done for {filename}")

if __name__ == "__main__":
    main()