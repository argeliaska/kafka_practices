import sys

from .consumer import Consumer

if __name__ == "__main__":
    consumer = Consumer(sys.argv[1])
    consumer.start_read()
