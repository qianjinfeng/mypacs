from pynetdicom import (
    AE, debug_logger, evt, AllStoragePresentationContexts,
    ALL_TRANSFER_SYNTAXES
)
import threading
from pika import ConnectionParameters, BlockingConnection, PlainCredentials
from http.server import BaseHTTPRequestHandler, HTTPServer
import json

# RabbitMQ 配置
RABBITMQ_HOST = 'localhost'


# debug_logger()

class Publisher(threading.Thread):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.daemon = True
        self.is_running = True
        self.name = "Publisher"
        self.queue = "downstream_queue"

        credentials = PlainCredentials("guest", "guest")
        parameters = ConnectionParameters("localhost", credentials=credentials)
        self.connection = BlockingConnection(parameters)
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue=self.queue, auto_delete=True)

    def run(self):
        while self.is_running:
            self.connection.process_data_events(time_limit=1)

    def _publish(self, message):
        self.channel.basic_publish("", routing_key=self.queue, body=message)
        print(f" [x] Sent '{message}' ")

    def publish(self, message):
        self.connection.add_callback_threadsafe(lambda: self._publish(message))

    def stop(self):
        print("Stopping...")
        self.is_running = False
        # Wait until all the data events have been processed
        self.connection.process_data_events(time_limit=1)
        if self.connection.is_open:
            self.connection.close()
        print("Stopped")

publisher = Publisher()

class RequestHandler(BaseHTTPRequestHandler):
    def do_POST(self):
        content_length = int(self.headers['Content-Length'])
        post_data = self.rfile.read(content_length).decode('utf-8')
        
        try:
            # 解析 JSON 数据
            data = json.loads(post_data)
        except (json.JSONDecodeError, KeyError) as e:
            self.send_response(400)
            self.send_header('Content-type', 'text/plain')
            self.end_headers()
            self.wfile.write(b'Invalid data format.')
            return
        
        publisher.publish(post_data)
        self.send_response(200)
        self.send_header('Content-type', 'text/plain')
        self.end_headers()
        self.wfile.write(b'Data already processed.')

def run(server_class=HTTPServer, handler_class=RequestHandler, port=8080):
    server_address = ('', port)
    httpd = server_class(server_address, handler_class)
    print(f'Starting httpd on port {port}...')
    httpd.serve_forever()

if __name__ == "__main__":
    
    publisher.start()

    try:
        run()
        i = 5
        msg = f"Message {i}"
        print(f"Publishing: {msg}")
        publisher.publish(msg)
        
    except KeyboardInterrupt:
        publisher.stop()
    finally:
        publisher.join()
