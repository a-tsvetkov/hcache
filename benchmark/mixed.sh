tcpkali -d -c10k -r10 -em '\{re (set [a-z]{3}[0-9][ ][a-z0-9]{1024}|get [a-z]{3}[0-9]|delete [a-z]{3}[0-9])}\n' --duration=300 --connect-rate=500 localhost:1488
