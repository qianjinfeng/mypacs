python -m pynetdicom storescu 127.0.0.1 11112 ~/Downloads/dicom/s/DICOM/24092403/54470000/27722412 -v -cx
python -m pynetdicom storescu 127.0.0.1 11112 ~/Downloads/dicom/3/series-000001/image-000001.dcm   -v -cx
python -m pynetdicom storescu 127.0.0.1 11112 ~/Downloads/dicom/2/DICOM/I3   -v -cx
python -m pynetdicom storescu 127.0.0.1 11112 ~/Downloads/dicom/1/series-000001/image-000003.dcm  -v -cx

python -m pynetdicom storescu 127.0.0.1 11112 ~/Downloads/dicom/s/DICOM/24092403/54470000 -r -v -cx

curl --header "Content-Type: application/json" --request POST --data '{"username":"xyz","password":"xyz"}' http://localhost:8080


IPFS private network
https://github.com/ipfs/kubo/blob/master/docs/experimental-features.md#private-networks
https://eleks.com/research/ipfs-network-data-replication/

go install github.com/Kubuxu/go-ipfs-swarm-key-gen/ipfs-swarm-key-gen@latest
ipfs-swarm-key-gen > ~/.ipfs/swarm.key

ipfs bootstrap rm --all
ipfs bootstrap add /ip4/<引导节点IP>/tcp/4001/ipfs/<引导节点PeerID>
export LIBP2P_FORCE_PNET=1
ipfs daemon

ipfs repo stat -H
ipfs pin ls -q --type recursive | xargs ipfs pin rm
ipfs repo gc 


"Discovery": {
  "MDNS": {
    "Enabled": false
  }
}
"Swarm": {
  "DisableAutoDial": true
}
"Routing": {
  "Type": "dht",
  "DHT": {
    "ReprovideInterval": "12h"
  }
}


tag is VR.UN VR.OW  not correct to json.dump  
~/Downloads/dicom/3/series-000001/image-000001.dcm