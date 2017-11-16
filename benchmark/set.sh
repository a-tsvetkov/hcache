for i in `seq 1 $1`
do LC_ALL=C tr -dc '[:alpha:]' </dev/urandom | head -c 100 | fold -w 20 | tr '\n' ' ' | awk '{print "set "$1 " " $2 $3 $4 $5}'
done > fixture.tmp

tcpkali -c$2 -r$3 -ef fixture.tmp --duration=60 localhost:1488

rm fixture.tmp
