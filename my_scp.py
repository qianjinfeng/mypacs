import os
from pynetdicom import (
    AE, debug_logger, evt, AllStoragePresentationContexts,
    ALL_TRANSFER_SYNTAXES
)
import threading
from pika import ConnectionParameters, BlockingConnection, PlainCredentials
from pathlib import Path
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
        self.queue = "dicom_queue"

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
        # print(f" [x] Sent '{message}' ")

    def publish(self, message):
        # print(f" [x] to publish '{message}' ")
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

class ExampleStorage(object):

    def __init__(self):
        self.ae = AE()
        storage_sop_classes = [
            cx.abstract_syntax for cx in AllStoragePresentationContexts
        ]
        for uid in storage_sop_classes:
            self.ae.add_supported_context(uid, ALL_TRANSFER_SYNTAXES)
        self.handlers = [(evt.EVT_C_STORE, self.handle_store)]
        self.currentSOPinstanceUID = ''
        self.instanceNR = ''

    def bulk_data_handler(self, data_element):
        # print(f'handler {data_element}')
        # try:
        #     os.makedirs(self.currentSOPinstanceUID, exist_ok=True)
        # except:
        #     # Unable to create output dir, return failure status
        #     return 0xC001

        # path = Path(storage_dir) / f"{uuid.uuid4()}"
        path = Path("/tmp/") / f"{self.currentSOPinstanceUID}"
        with path.open('w') as f:
            # Write the preamble, prefix, file meta information elements
            #   and the raw encoded dataset to `f`
            # f.write(data_element.pixel_array.tobytes())
            json.dump(data_element.to_json(), f, indent=4)

        return "binary_uri"

    def handle_store(self, event):
        """Handle EVT_C_STORE events."""
        ds = event.dataset
        ds.file_meta = event.file_meta

        self.currentSOPinstanceUID = ds['SOPInstanceUID'].value
        self.instanceNR = ds['InstanceNumber'].value
        ds_json = ds.to_json(128, bulk_data_element_handler=self.bulk_data_handler)

        print(f'received SOP instance UID {self.currentSOPinstanceUID}')
        # print(f'received instance Number {self.instanceNR}')
        # print(f'received patient  {ds['PatientName']}')

        # print(f'received json {ds_json}')
        publisher.publish(ds_json)
        return 0x0000

    def run(self):
        self.ae.start_server(("127.0.0.1", 11112), block=True, evt_handlers=self.handlers)

if __name__ == "__main__":
    publisher.start()
    storager = ExampleStorage()

    try:
        storager.run()
    except KeyboardInterrupt:
        publisher.stop()
    finally:
        publisher.join()
