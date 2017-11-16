tcpkali -d -c10k -r1 -em 'set \{re [a-z0-9]{4} } \{re [a-z0-9]{1024} }\n' --duration=300 --connect-rate=400 localhost:1488
