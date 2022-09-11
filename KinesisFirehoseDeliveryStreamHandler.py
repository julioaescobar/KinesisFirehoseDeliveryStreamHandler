import boto3
import logging


class KinesisFirehoseDeliveryStreamHandler(logging.StreamHandler):

    def __init__(self):
        # By default, logging.StreamHandler uses sys.stderr if stream parameter is not specified
        logging.StreamHandler.__init__(self)

        self.__firehose = None
        self.__stream_buffer = []

        try:
            self.__firehose = boto3.client('firehose')
        except Exception:
            print('Firehose client initialization failed.')

        self.__delivery_stream_name = "Chat.Connector-Input.Message.Management"

    def emit(self, record):
        try:
            msg = self.format(record)

            if self.__firehose:
                self.__stream_buffer.append({
                    'Data': msg.encode(encoding="UTF-8", errors="strict")
                })
            else:
                stream = self.stream
                stream.write(msg)
                stream.write(self.terminator)

            self.flush()
        except Exception:
            self.handleError(record)

    def flush(self):
        self.acquire()

        try:
            if self.__firehose and self.__stream_buffer:
                self.__firehose.put_record_batch(
                    DeliveryStreamName=self.__delivery_stream_name,
                    Records=self.__stream_buffer
                )

                self.__stream_buffer.clear()
        except Exception as e:
            print("An error occurred during flush operation.")
            print(f"Exception: {e}")
            print(f"Stream buffer: {self.__stream_buffer}")
        finally:
            if self.stream and hasattr(self.stream, "flush"):
                self.stream.flush()

            self.release()