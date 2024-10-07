from pydicom import dcmread
from pynetdicom import AE, StoragePresentationContexts
from pydicom.uid import ExplicitVRLittleEndian, ImplicitVRLittleEndian
import os
import time

# 创建应用实体 (AE)
ae = AE()

# transfer_syntax = '1.2.840.10008.1.2.1'  # ExplicitVRLittleEndian  

# 添加支持的存储上下文
for context in StoragePresentationContexts:
    ae.add_requested_context(context.abstract_syntax, context.transfer_syntax)

# 设置目标 SCP 的 IP 地址和端口
peer_ip = '127.0.0.1'
peer_port = 11112

# 连接到 SCP
assoc = ae.associate(peer_ip, peer_port)

if assoc.is_established:
    # 指定 DICOM 文件夹路径
    dicom_dir = '/home/qian/Downloads/dicom/s/DICOM/24092403/54470000/'
    # dicom_dir = '/home/qian/Downloads/dicom/1/series-000001/'

    
    # 遍历文件夹中的所有文件
    for root, dirs, files in os.walk(dicom_dir):
        for file_name in files:
            file_path = os.path.join(root, file_name)
            print(f'Sending {file_path}...')
            
            time.sleep(5)

            # 读取 DICOM 文件
            ds = dcmread(file_path)
            print(f'Read SOPInstanceUID: {ds.SOPInstanceUID}...')
            # print(f'tag: {ds[0x0019,0x1002]}')
            ds.file_meta.TransferSyntaxUID = ExplicitVRLittleEndian

            # 发送 DICOM 文件
            status = assoc.send_c_store(ds)
            if status:
                print('C-STORE request succeeded')
            else:
                print('C-STORE request failed')

    # 断开连接
    assoc.release()
else:
    print('Association rejected, aborted or never connected')