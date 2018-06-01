from __future__ import print_function
import Pyro4
import threading
from multiprocessing.dummy import Pool
pool = Pool(processes=3)
from functools import partial
import socket
import time


server_uri_1 = "PYRO:server1@10.0.0.2:4100"
server_uri_2 = "PYRO:server2@10.0.0.2:4200"
server_uri_3 = "PYRO:server2@10.0.0.2:4300"

lock = threading.Condition()
list = []

@Pyro4.expose
class ServerOps(object):


    def __init__(self):
        self.server_list = [server_uri_1, server_uri_2]
        self.serverLockList = {url: [] for url in self.server_list}
        self.critical = threading.Semaphore(1)

    def removeServer(self, url):
        self.server_list.remove(url)

    def releaseAccountWithCnt(self,state, serveruri, ac_num):
        self.critical.acquire()

        print(serveruri, "  releasing account: ", ac_num)
        list = self.serverLockList[serveruri]
        list.remove(ac_num)
        self.serverLockList[serveruri] = list

        allUpdateComplete = True
        for serveruri in self.serverLockList:
            if ac_num in self.serverLockList[serveruri]:
                allUpdateComplete = False
                break

        if allUpdateComplete:
            print("AllupdateComplete")
            self.releaseLock(accountId=ac_num)

        self.critical.release()

    def acquireLock(self, accountId):
        with lock:
            while True:
                if accountId not in list:
                    break
                print("waiting", accountId)
                lock.wait(5)
            print("Lock Acquired",accountId)
            list.append(accountId)
            return

    def releaseLock(self, accountId):
        with lock:
            list.remove(accountId)
            lock.notify_all()
        return

    @staticmethod
    def get_ip_address():
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(('8.8.8.8', 1))  # connect() for UDP doesn't send packets
        return s.getsockname()[0]


@Pyro4.expose
class Coordinator(object):

    def __init__(self):
        self.serverOps = ServerOps()
        self.resyncOnProgress = False
        t = threading.Thread(target=self.heartBeat, args=())
        t.start()

    def resync(self):
        self.resyncOnProgress = True

    def doneresync(self):
        self.resyncOnProgress = False

    def deposit(self, accountId, amount):
        while self.resyncOnProgress:
            pass

        self.serverOps.acquireLock(accountId=accountId)
        for url in self.serverOps.server_list:
            try:
                out = self.serverOps.serverLockList[url]
                out.append(accountId)
                self.serverOps.serverLockList[url] = out
            except:
                print("exceptions")

        for url in self.serverOps.server_list:
            server = Pyro4.Proxy(url)
            print("server : ",server)
            deposit_callback = partial(self.serverOps.releaseAccountWithCnt, serveruri=url,ac_num=accountId)
            pool.apply_async(server.deposit, args=[accountId,amount], callback= deposit_callback)

    def withdraw(self, accountId, amount):

        while self.resyncOnProgress:
            pass

        self.serverOps.acquireLock(accountId=accountId)
        for url in self.serverOps.server_list:
            try:
                out = self.serverOps.serverLockList[url]
                out.append(accountId)
                self.serverOps.serverLockList[url] = out
            except:
                print("exceptions")

        for url in self.serverOps.server_list:
            server = Pyro4.Proxy(url)
            print("server",server)
            withdraw_callback = partial(self.serverOps.releaseAccountWithCnt, serveruri=url,ac_num=accountId)
            pool.apply_async(server.withdraw, args=[accountId,amount], callback= withdraw_callback)

    def getBalance(self,accountId):

        while self.resyncOnProgress:
            pass

        server = Pyro4.Proxy(self.serverOps.server_list[0])
        try:
            return server.getBalance(accountId)
        except:
            return

    def heartBeat(self):
        # This function runs in a seperate thread , checks for each server availability 1 sec interval
        while True:
            #print("HEART BEAT")
            for url in self.serverOps.server_list:
                try:
                    server = Pyro4.Proxy(url)
                    server.isalive()  # check if the server is alive
                except:
                    print(uri, "is dead,releasing its held accounts")
                    for acc in self.serverOps.serverLockList[url]:  # release the locked accounts by this server
                        self.serverOps.releaseAccountWithCnt(None, url, acc)
                    self.serverOps.server_list.remove(url)
            time.sleep(1)

    def sendOtherServerUris(self, serveruri):
        # A failed server is requesting for the other alive servers.
        uris = []
        for url in self.serverOps.server_list:
            if url != serveruri:
                uris.append(url)
        self.serverOps.server_list.append(serveruri)
        return uris


def main():
    coord = Coordinator()
    Ip = ServerOps.get_ip_address()
    print("IP Address ", Ip)
    Pyro4.Daemon.serveSimple(host=Ip,
                             objects ={
                                 coord : "coordinator"
                             },
                             port=3100,
                             ns = False)


if __name__ == '__main__':
    main()