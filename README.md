# simplep2p
simple peer to peer application in Java


java FileOwner.java <fileowner_port> 
```
Command line arguments were not provided. Setting to default values: localhost,8000
enter number of peers [5]:

setting the default value:5
enter chunk size [100000]:

setting the default value:100000
enter the file path [/Users/apple/Desktop/test.pdf]:

setting the default value:/Users/apple/Desktop/test.pdf
starting server in address:localhost port:8000
[server] starting chuck initialization..
[server] file size: 4303180 chunk size:100000 # of chunks:43
[server] chuck initialization complete
[server] waiting for new connection..
```

java Peer.java <fileowner_port> <port> <nighbour_peer_port>
