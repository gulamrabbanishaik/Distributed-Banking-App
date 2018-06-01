import Pyro4


def main():
    coordinaotrIp = "10.0.0.2"
    uri = "PYRO:coordinator@" + coordinaotrIp + ":3100"
    coordinator = Pyro4.Proxy(uri)

    while True:

        op = input("Select op (d,w,g) : ").strip()

        if op is 'd':
            accountId = input("Type account Number: ").strip()
            amount = input("Type amount: ").strip()
            id = int(accountId)
            amt = int(amount)
            coordinator.deposit(id, amt)
            """
            for i in range(1,5):
                id = random.randint(1001,1004)
                amt = random.randint(1,6) * 10
                print(id,":",amt)
                coordinator.deposit(id, amt)
            """

        elif op is 'w':
            accountId = input("Type account Number: ").strip()
            amount = input("Type amount: ").strip()
            id = int(accountId)
            amt = int(amount)
            coordinator.withdraw(id, amt)

        else:
            accountId = input("Type account Number: ").strip()
            id = int(accountId)
            print('Your Balance is ', coordinator.getBalance(id))

        print("**************************************************************")


if __name__ == '__main__':
    main()