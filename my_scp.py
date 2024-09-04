 import uuid
 from pathlib import Path

 from pynetdicom import (
     AE, debug_logger, evt, AllStoragePresentationContexts,
     ALL_TRANSFER_SYNTAXES
 )

 debug_logger()

 def handle_store(event, storage_dir):
     """Handle EVT_C_STORE events."""
     try:
         os.makedirs(storage_dir, exist_ok=True)
     except:
         # Unable to create output dir, return failure status
         return 0xC001

     path = Path(storage_dir) / f"{uuid.uuid4()}"
     with path.open('wb') as f:
         # Write the preamble, prefix, file meta information elements
         #   and the raw encoded dataset to `f`
         f.write(event.encoded_dataset())

     return 0x0000

 handlers = [(evt.EVT_C_STORE, handle_store, ['out'])]

 ae = AE()
 storage_sop_classes = [
     cx.abstract_syntax for cx in AllStoragePresentationContexts
 ]
 for uid in storage_sop_classes:
     ae.add_supported_context(uid, ALL_TRANSFER_SYNTAXES)

 ae.start_server(("127.0.0.1", 11112), block=True, evt_handlers=handlers)