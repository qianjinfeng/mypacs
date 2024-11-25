import os
from pynetdicom import (
    AE, debug_logger, evt, AllStoragePresentationContexts,
    ALL_TRANSFER_SYNTAXES
)
from pynetdicom.sop_class import Verification
from pydicom.tag import Tag
import threading
from pika import ConnectionParameters, BlockingConnection, PlainCredentials
from pathlib import Path
import json
import requests
from io import BytesIO

# RabbitMQ 配置
RABBITMQ_HOST = 'localhost'
# 设置RabbitMQ连接
credentials = PlainCredentials("guest", "guest")
parameters = ConnectionParameters("localhost", credentials=credentials)
rabbitmq_connection = BlockingConnection(parameters)
rabbitmq_channel = rabbitmq_connection.channel()
rabbitmq_channel.queue_declare(queue='dicom_queue', auto_delete=True)

# debug_logger()

# class Publisher(threading.Thread):
#     def __init__(self, *args, **kwargs):
#         super().__init__(*args, **kwargs)
#         self.daemon = True
#         self.is_running = True
#         self.name = "Publisher"
#         self.queue = "dicom_queue"

#         credentials = PlainCredentials("guest", "guest")
#         parameters = ConnectionParameters("localhost", credentials=credentials)
#         self.connection = BlockingConnection(parameters)
#         self.channel = self.connection.channel()
#         self.channel.queue_declare(queue=self.queue, auto_delete=True)

#     def run(self):
#         while self.is_running:
#             self.connection.process_data_events(time_limit=1)

#     def _publish(self, message):
#         self.channel.basic_publish("", routing_key=self.queue, body=message)
#         # print(f" [x] Sent '{message}' ")

#     def publish(self, message):
#         # print(f" [x] to publish '{message}' ")
#         self.connection.add_callback_threadsafe(lambda: self._publish(message))

#     def stop(self):
#         print("Stopping...")
#         self.is_running = False
#         # Wait until all the data events have been processed
#         self.connection.process_data_events(time_limit=1)
#         if self.connection.is_open:
#             self.connection.close()
#         print("Stopped")

# publisher = Publisher()

class ExampleStorage(object):

    def __init__(self):
        self.ae = AE(ae_title='MY_STORE_SCU')
        storage_sop_classes = [
            cx.abstract_syntax for cx in AllStoragePresentationContexts
        ]
        for uid in storage_sop_classes:
            self.ae.add_supported_context(uid, ALL_TRANSFER_SYNTAXES)
        self.ae.add_supported_context(Verification, ALL_TRANSFER_SYNTAXES)
        self.handlers = [
            (evt.EVT_C_STORE, self.handle_store),
            (evt.EVT_C_ECHO, self.handle_echo) ]
        self.currentSOPinstanceUID = ''
        # Kubo API 地址，这里假设Kubo守护进程运行在本地，并且使用默认端口5001
        self.api_url = 'http://127.0.0.1:5001/api/v0/add'
        self.instanceNR = ''

    # 处理C-ECHO请求
    def handle_echo(event):
        print("Received C-ECHO request")
        return 0x0000  # 返回状态码，0x0000表示成功
    
    def bulk_data_handler(self, data_element):
        if data_element.VR in ['OB', 'OD', 'OF', 'OL', 'OV','OW']:
            file_name = f'{data_element.tag:08x}'  # 将tag转换为十六进制字符串
            # 准备POST请求的数据
            files = {'file': (file_name, data_element.value)}
            try:
                # 发送POST请求
                response = requests.post(self.api_url, files=files)
                response.raise_for_status()  # 抛出HTTP错误
            except Exception as e:
                print(f"Failed to store DICOM image to IPFS: {e}")
            
            # 检查响应状态码
            if response.status_code == 200:
                result = response.json()
                print(f'BulkData added to IPFS: {result["Hash"]}')
                return result['Hash']
            else:
                print(f'Error adding BulkData to IPFS: {response.status_code} - {response.text}')

        return data_element.value
        # return "blk_url"

    def handle_store(self, event):
        """Handle EVT_C_STORE events."""
        ds = event.dataset
        ds.file_meta = event.file_meta

        self.currentSOPinstanceUID = ds['SOPInstanceUID'].value
        self.instanceNR = ds['InstanceNumber'].value
        
        # ds_json = ds.to_json(64, bulk_data_element_handler=self.bulk_data_handler)
        ds_dict = ds.to_json_dict(512, bulk_data_element_handler=self.bulk_data_handler)
        meta_dict = ds.file_meta.to_json_dict()
        # 合并 FileMetaInformation 和 Dataset
        combined_dict = {**ds_dict, **meta_dict}
        # TODO: exception handling
        ds_json = json.dumps(combined_dict, indent=4)

        print(f'received SOP instance UID {self.currentSOPinstanceUID}')
        # print(f'received instance Number {self.instanceNR}')
        # print(f'received patient  {ds['PatientName']}')

        # print(f'received json {ds_json}')
        # publisher.publish(ds_json)
        rabbitmq_channel.basic_publish(exchange='', routing_key='dicom_queue', body=ds_json)
        return 0x0000

    def run(self):
        self.ae.start_server(("127.0.0.1", 11112), block=True, evt_handlers=self.handlers)

if __name__ == "__main__":
    # publisher.start()
    storager = ExampleStorage()

    try:

        storager.run()
        
    except KeyboardInterrupt:
        # publisher.stop()
        rabbitmq_connection.close()
    finally:
        # publisher.join()
        rabbitmq_connection.close()
