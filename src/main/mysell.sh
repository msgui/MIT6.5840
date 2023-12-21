#!/usr/bin/env bash



rm workerOut/mr-out-*
go build -buildmode=plugin ../mrapps/wc.go

go run mrsequential.go wc.so pg-*.txt


for ((i=1;i<=300;i++))
do
(
  go run mrworker.go wc.so

) &
done
wait
echo -E "########## SECONDS ##########"