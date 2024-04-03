.PHONY: coord  worker
coord:
	go build -buildmode=plugin ../mrapps/wc.go && \
	rm -f mr-out* && \
	go run mrcoordinator.go pg-*.txt

worker:
	go run mrworker.go wc.so



