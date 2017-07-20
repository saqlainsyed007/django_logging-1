from logging.handlers import SocketHandler
from logstash import formatter
import json


class SocketLogstashHandler(SocketHandler):
    """
        Socket based logstash handler.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        try:
            self.sock = self.makeSocket()
            print("Logstash Connected")
        except Exception:
            print("Logstash Connection Failed")

    def emit(self, record):
        record.msg = self.format(record) + '\n'
        logstash_formatter = formatter.LogstashFormatterVersion1(
            'logstash',
            getattr(self, 'tags', []),
            getattr(self, 'fqdn', False)
        )
        message = logstash_formatter.format(record)
        message_string = message.decode()
        message_dict = json.loads(message_string)
        message_dict['type'] = 'django_log'
        message = json.dumps(message_dict) + '\n'
        if self.sock is not None:
            try:
                x = self.sock.send(message.encode())
                print(x)
            except BaseException as e:
                print(e)
        else:
            print("Logstash Connection Failed")
