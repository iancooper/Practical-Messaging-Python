from pubsubchannel.pubsub import pubsub, ChannelType


def run():
    with pubsub(ChannelType.Publisher) as channel:
        message = "Hello World"
        channel.send(message)
        print("Sent Message: ", message)

    input("Press [enter] to exit")


if __name__ == "__main__":
    run()




