import time

from pubsubchannel.pubsub import pubsub, ChannelType


def run():
    while True:
        try:
            with pubsub(ChannelType.Subscriber) as channel:
                print("Pausing for breath...")
                time.sleep(4)

                message = channel.receive()
                if message is not None:
                    print("Received message", message)
                else:
                    print("Did not receive message")

                print("Press <CTRL+C> to exit.")
        except KeyboardInterrupt:
            break


if __name__ == "__main__":
    run()
