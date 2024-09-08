from pynetdicom import (
    AE, debug_logger, evt, AllStoragePresentationContexts,
    ALL_TRANSFER_SYNTAXES
)
import pika


# debug_logger()

def bulk_data_handler(data_element):
    return "binary_uri"

def handle_store(event, storage_dir):
    """Handle EVT_C_STORE events."""
    ds = event.dataset
    ds.file_meta = event.file_meta
    
    ds_json = ds.to_json_dict(128, bulk_data_element_handler=bulk_data_handler)

    print(f'received json {ds_json}')

    try:
        channel.basic_publish(exchange='direct_dicom', routing_key=ds['StudyInstanceUID'].value, body=ds_json)
        print('published to mq')
    except:
        return 0xC001

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

connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='localhost'))
channel = connection.channel()

channel.exchange_declare(exchange='direct_dicom', exchange_type='direct')


ae.start_server(("127.0.0.1", 11112), block=True, evt_handlers=handlers)
