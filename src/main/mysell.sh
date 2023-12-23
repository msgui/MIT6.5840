#!/usr/bin/env bash


# shellcheck disable=SC2034
PLUGIN="indexer"
rm mr-*
rm reduce-*
go build -buildmode=plugin ../mrapps/"$PLUGIN".go

for ((i=1;i<=4;i++))
do
(
if [ "$i" -eq 1 ]
then
  go run mrcoordinator.go pg-*.txt
else
  go run mrworker.go "$PLUGIN".so
fi

) &
done
wait
sort mr-out* | grep . > mr-"$PLUGIN"-all
go build -buildmode=plugin ../mrapps/"$PLUGIN".go
go run mrsequential.go "$PLUGIN".so pg-*.txt
sort mr-out-0 | grep . > mr-correct

if  cmp mr-"$PLUGIN"-all  mr-correct
then
  echo '--- 文件内容相同'
else
  echo '--- 文件内容有误'
fi

echo -E "########## SECONDS ##########"