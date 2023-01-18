master slave architecture

Node 
1. Named Node
2. Data Node 

Other function
1. Periodically Block Report

How client download files:
1. send request to Named Node
2. Named Node send back the server that have this file
3. Client download the file from that server

How client upload files:
1. send request to Named Node
2. Named Node send back list of Data Nodes that should contains the files
3. Client send file to Data Nodes
4. Client told server that this is complete
5. As long as one Datanodes successfully get the file, client told the Named Node that job complete
6. Server update file map