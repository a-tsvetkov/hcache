for i in `seq 1 10000`; do echo "incr c 1"; done | xargs -I{} -P 128 sh -c  'echo "{}" | nc localhost 1488'
