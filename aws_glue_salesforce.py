import sys
from simple_salesforce import Salesforce
"""
JOB PARAMATERS 
KEY: --additional-python-modules
VALUE: cryptography==3.0,simple-salesforce==1.11.1
"""
def main():
    print("INIT")
    sf = Salesforce(username='username', password='pw', security_token='securitytoken', domain='test')
    res_bulk = sf.bulk.Account.query('SELECT Id, Name FROM Table')
    print(res_bulk)

if __name__ == "__main__":
    main()