from pynetdicom import (
    AE, debug_logger, evt, AllStoragePresentationContexts,
    ALL_TRANSFER_SYNTAXES
)
import threading
import pika

# 用于跟踪已经处理的 key 和消费者线程
processed_studies = set()
# consumers = {}
# consumers_lock = threading.Lock()

# RabbitMQ 配置
RABBITMQ_HOST = 'localhost'

# debug_logger()

def rabbitmq_consumer(queue_name):
    """ 从指定的 RabbitMQ 队列中消费消息 """
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
    channel = connection.channel()
    channel.queue_declare(queue=queue_name)
    
    def callback(ch, method, properties, body):
        print(f"Processing message from queue {queue_name}: {body}")

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

def bulk_data_handler(data_element):
    return "binary_uri"

def handle_store(event, storage_dir):
    """Handle EVT_C_STORE events."""
    ds = event.dataset
    ds.file_meta = event.file_meta
    
    ds_json = ds.to_json_dict(128, bulk_data_element_handler=bulk_data_handler)

    # print(f'received json {ds_json}')
    aStudyInstanceUID = ds['StudyInstanceUID'].value
    if aStudyInstanceUID in processed_studies:
        send_to_rabbitmq(aStudyInstanceUID, ds_json)
        print('published to mq')
    else:
        send_to_rabbitmq(aStudyInstanceUID, ds_json)

        # 启动消费者线程来处理消息
        # with consumers_lock:
        #     if aStudyInstanceUID not in consumers:
        #         start_consumer_thread(aStudyInstanceUID)
        start_consumer_thread(aStudyInstanceUID)
        
        # 将 key 标记为已处理
        processed_studies.add(aStudyInstanceUID)

#  try:
#      os.makedirs(storage_dir, exist_ok=True)
#  except:
#      # Unable to create output dir, return failure status
#      return 0xC001

#  path = Path(storage_dir) / f"{uuid.uuid4()}"
#  with path.open('wb') as f:
#      # Write the preamble, prefix, file meta information elements
#      #   and the raw encoded dataset to `f`
#      f.write(event.encoded_dataset())

    return 0x0000

handlers = [(evt.EVT_C_STORE, handle_store, ['out'])]

ae = AE()
storage_sop_classes = [
    cx.abstract_syntax for cx in AllStoragePresentationContexts
]
for uid in storage_sop_classes:
    ae.add_supported_context(uid, ALL_TRANSFER_SYNTAXES)


ae.start_server(("127.0.0.1", 11112), block=True, evt_handlers=handlers)
