python -m pynetdicom storescu 127.0.0.1 11112 ~/Downloads/2_skull_ct/DICOM/I1 -v -cx

curl --header "Content-Type: application/json" --request POST --data '{"username":"xyz","password":"xyz"}' http://localhost:8080