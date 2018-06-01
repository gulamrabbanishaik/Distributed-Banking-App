from __future__ import print_function
import Pyro4
import socket
import time
import os
import json

def get_ip_address():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.connect(("8.8.8.8",80))
    return s.getsockname()[0]

coordinator_uri = "PYRO:coordinator@10.0.0.2:3100"


@Pyro4.expose
class Db(object):
    def __init__(self,filename):
        self.dbfile = 'data'+str(filename)
        initialDb = {"1001":0,"1002":0,"1003":0,"1004":0}
        fp = open(self.dbfile,'w')
        json.dump(initialDb,fp)
        fp.close()

    def setBalance(self,acc_number,baln):
        fp = open(self.dbfile, 'r')
        cur_db = json.load(fp)
        cur_db[acc_number] = baln

        fp = open(self.dbfile, 'w')
        json.dump(cur_db, fp)
        fp.close()



    def getBalance(self,acc_number):
        acc_number = str(acc_number)
        fp = open(self.dbfile,'r')
        cur_db = json.load(fp)
        fp.close()
        return cur_db[acc_number]


    def deposit(self,acc_number,amnt):
        acc_number = str(acc_number)

        fp = open(self.dbfile, 'r')
        cur_db = json.load(fp)
        fp.close()

        cur_db[acc_number] += amnt

        fp = open(self.dbfile, 'w')
        json.dump(cur_db, fp)
        fp.close()
        return cur_db[acc_number]

    def withdraw(self, acc_number, amnt):
        acc_number = str(acc_number)

        fp = open(self.dbfile, 'r')
        cur_db = json.load(fp)
        fp.close()

        if amnt > cur_db[acc_number]:
            return -1

        cur_db[acc_number] -= amnt
        print("cur_db : ", cur_db[acc_number])
        fp = open(self.dbfile, 'w')
        json.dump(cur_db, fp)
        fp.close()
        return cur_db[acc_number]



@Pyro4.expose
class Server(object):


    def __init__(self, id, port):

        self.myuri = "PYRO:server" + str(id) + "@" + get_ip_address() + ":" + str(port)
        self.db = Db(id)
        coordinator = Pyro4.Proxy(coordinator_uri)
        self.logfilename = "log" + str(id)
        if os.path.isfile(self.logfilename):
            self.resynchronize(coordinator)
        # operation count
        self.optn_count = 0

    def tail(self, filename):
        lastline = ''
        with open(filename, 'r') as fp:
            for line in fp:
                lastline = line
        return lastline

    def resynchronize(self,coordinator):
        print("Starting sync")
        coordinator.resync() #Equivalent to sending "ALIVE" message in the assignment

        otherServers = coordinator.sendOtherServerUris(self.myuri)

        lastline = self.tail(self.logfilename)
        print(lastline)
        if len(lastline) == 0: #nothing to sync
            lastline = '-1 0 0'
        for server_name in otherServers:
            print(server_name)
            server = Pyro4.Proxy(server_name)
            print("0")
            operationsNotSeen = server.sendLog(lastline) #operationsNotSeen is a dictionary of accounts and tuples equivalent to "resynchronize" message in the server
            print(operationsNotSeen)
            for acc in operationsNotSeen:
                self.db.setBalance(acc,operationsNotSeen[acc])
        #Flush the logfile
        open(self.logfilename, 'w').close()
        coordinator.doneresync() #Equivalent to the "RESYNCH-DONE" message


    def sendLog(self,lastLine):
        # A server has requested the log after failing , lastLine is the last line of that server
        print(lastLine)
        lastOp = int(lastLine.strip().split()[0])
        updatedAcnts = {} #holds the latest balance of the accounts

        with open(self.logfilename,'r') as fp:

            for line in fp:
                print(line)
                l = line.strip().split()
                if int(l[0]) > lastOp: #if this operation number is greater , then we should update it
                    updatedAcnts[l[1]] = int(l[2])
        #We have an updated server , now flush the log
        open(self.logfilename, 'w').close()
        return updatedAcnts



    def deposit(self, accountId, amount):
        print("Deposit :  ", accountId)
        self.optn_count += 1
        cur_balance = self.db.deposit(acc_number=accountId, amnt=amount)

        print("Current Balance : ",cur_balance)
        with open(self.logfilename, 'a') as fp:
            fp.write(str(self.optn_count) + " " + str(accountId) + " " + str(cur_balance) + "\n")

        time.sleep(5)  # simulate , wait for 5 minutes to see the effect
        return cur_balance

    def withdraw(self, accountId, amount):
        print("Withdraw :  ",accountId)
        self.optn_count += 1
        cur_balance = self.db.withdraw(acc_number=accountId, amnt=amount)
        if (cur_balance < 0):
            #print("Server#" + str(self.id) + " failed to withdraw , not enough balance")
            return -1
        with open(self.logfilename, 'a') as fp:
            fp.write(str(self.optn_count) + " " + str(accountId) + " " + str(cur_balance) + "\n")
        time.sleep(5)  # simulate , wait for 5 minutes to see the effect
        return cur_balance

    def getBalance(self, accountId):
        print("Get Balance :  ", accountId)
        return self.db.getBalance(accountId)

    def isalive(self):
        return True

def main():
    s = input("Please enter server id and port: ").strip().split()
    id = int(s[0])
    port = int(s[1])
    server = Server(id=id, port=port)
    Ip = get_ip_address()
    Pyro4.Daemon.serveSimple(host=Ip,
                             objects ={
                                 server: "server" + str(id)
                             },
                             port=port,
                             ns = False)


if __name__ == '__main__':
    main()