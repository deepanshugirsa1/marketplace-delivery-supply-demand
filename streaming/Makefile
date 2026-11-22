SHELL := /bin/bash

up:                ## start Kafka, Flink (2 taskmanagers), ClickHouse
	docker compose up -d --build
	@echo "Flink UI  http://localhost:8081"
	@echo "ClickHouse http://localhost:8123/play"

submit:            ## submit the streaming job to the Flink cluster
	docker compose exec jobmanager flink run -d -py /opt/jobs/supply_demand_stream.py

produce:           ## replay 100k orders (~400k events) into Kafka
	python producer/produce_events.py --orders 100000

bench:             ## end-to-end throughput benchmark -> artifacts/
	python benchmark.py --orders 250000

verify:            ## show the latest windows that landed
	@curl -s "http://localhost:8123/?query=SELECT%20city,window_start,orders_placed,active_couriers,round(undersupply_rate,3),round(on_time_rate,3)%20FROM%20marketplace.city_minute_metrics%20ORDER%20BY%20window_start%20DESC%20LIMIT%2015%20FORMAT%20PrettyCompact"

kill-taskmanager:  ## chaos test: kill a taskmanager, watch the job recover from checkpoint
	docker kill mk-taskmanager || docker compose kill taskmanager
	@echo "watch recovery at http://localhost:8081 -- then re-run 'make verify'"

down:
	docker compose down -v

.PHONY: up submit produce bench verify kill-taskmanager down
