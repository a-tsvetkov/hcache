echo "Creaging $1 random records"
for i in `seq 1 $1`
do LC_ALL=C tr -dc '[:alpha:]' </dev/urandom | head -c 100 | fold -w 20 | tr '\n' ' ' | awk '{print "set "$1 " " $2 $3 $4 $5}'
done | nc localhost 1488 > /dev/null

echo "Creating counter record key-magicounter"
echo "set magiccounter 0" | nc localhost 1488

tcpkali -c$2 -r$3 -em 'incr magiccounter 1\n' --duration=60 localhost:1488
