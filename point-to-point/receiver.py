from p2pchannel.p2p import p2p


def run():
    with p2p("hello-p2p") as channel:
        message = channel.receive()
        if message is not None:
            print("Received message %s", message)
        else:
            print("Did not receive message")

    input("Press <enter> to exit.")

if __name__ == "__main__":
    run()
