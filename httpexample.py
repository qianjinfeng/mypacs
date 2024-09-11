from http.server import BaseHTTPRequestHandler, HTTPServer
import threading
import pika
import json

# 用于跟踪已经处理的 key 和消费者线程
processed_keys = set()
consumers = {}
consumers_lock = threading.Lock()

# RabbitMQ 配置
RABBITMQ_HOST = 'localhost'

def rabbitmq_consumer(queue_name):
    """ 从指定的 RabbitMQ 队列中消费消息 """
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
    channel = connection.channel()
    channel.queue_declare(queue=queue_name)
    
    def callback(ch, method, properties, body):
        message = body.decode('utf-8')
        if message not in processed_keys:
            print(f"Processing message from queue {queue_name}: {message}")
            # 处理消息（可以在这里添加更多处理逻辑）
        else:
            print(f"Message already processed: {message}")

    channel.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=True)
    print(f'Waiting for messages in queue {queue_name}. To exit press CTRL+C')
    
    # 通过无限循环保持消费者线程活跃
    channel.start_consuming()

def start_consumer_thread(queue_name):
    """ 启动新的消费者线程 """
    consumer_thread = threading.Thread(target=rabbitmq_consumer, args=(queue_name,))
    consumer_thread.daemon = True
    consumer_thread.start()
    
    # # 添加到消费者字典
    # with consumers_lock:
    #     consumers[queue_name] = consumer_thread

def send_to_rabbitmq(queue_name, message):
    """ 将消息发送到 RabbitMQ 队列 """
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
    channel = connection.channel()
    channel.queue_declare(queue=queue_name)
    channel.basic_publish(exchange='', routing_key=queue_name, body=message)
    print(f" [x] Sent '{message}' to queue '{queue_name}'")
    connection.close()

class RequestHandler(BaseHTTPRequestHandler):
    def do_POST(self):
        content_length = int(self.headers['Content-Length'])
        post_data = self.rfile.read(content_length).decode('utf-8')
        
        try:
            # 解析 JSON 数据
            data = json.loads(post_data)
            key = data['key']
            content = data['content']
        except (json.JSONDecodeError, KeyError) as e:
            self.send_response(400)
            self.send_header('Content-type', 'text/plain')
            self.end_headers()
            self.wfile.write(b'Invalid data format.')
            return
        
        if key in processed_keys:
            # 将消息发送到 RabbitMQ 队列
            send_to_rabbitmq(key, content)

            self.send_response(200)
            self.send_header('Content-type', 'text/plain')
            self.end_headers()
            self.wfile.write(b'Data already processed.')

        else:
            # 将消息发送到 RabbitMQ 队列
            send_to_rabbitmq(key, content)
            
            # 启动消费者线程来处理消息
            # with consumers_lock:
            #     if key not in consumers:
            #         start_consumer_thread(key)
            start_consumer_thread(key)
            
            # 将 key 标记为已处理
            processed_keys.add(key)
            
            # 发送响应
            self.send_response(200)
            self.send_header('Content-type', 'text/plain')
            self.end_headers()
            self.wfile.write(b'Received new data, processing...')

def run(server_class=HTTPServer, handler_class=RequestHandler, port=8080):
    server_address = ('', port)
    httpd = server_class(server_address, handler_class)
    print(f'Starting httpd on port {port}...')
    httpd.serve_forever()

if __name__ == "__main__":
    run()
