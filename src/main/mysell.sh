#!/usr/bin/env bash



rm mr-out-*
go build -buildmode=plugin ../mrapps/wc.go

go run mrsequential.go wc.so a.txt


for ((i=1;i<=3;i++))
do
(
  go run mrworker.go wc.so

) &
done
wait
echo -E "########## SECONDS ##########"