from p2pchannel.p2p import p2p


def run():
    with p2p("hello-p2p") as channel:
        message = "Hello World"
        channel.send(message)
        print("Sent Message %s", message)

    input("Press [enter] to exit")

if __name__ == "__main__":
    run()




