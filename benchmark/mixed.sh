tcpkali -d -c15k -r1 -em '\{re (set [a-z0-9]{4}[ ][a-z0-9]{32}|get [a-z0-9]{4})}\n' --duration=300 --connect-rate=200 localhost:1488
