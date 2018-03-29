from p2pchannel.p2p import p2p, ChannelType


def run():
    with p2p(ChannelType.Publisher) as channel:
        message = "Hello World"
        channel.send(message)
        print("Sent Message: ", message)

    input("Press [enter] to exit")

if __name__ == "__main__":
    run()




