.PHONY: docker-clean
docker-clean:
	@docker compose down -v --remove-orphans

.PHONY: docker-test
docker-test:
	@docker compose build test && docker compose run --rm -e GODEBUG=cgocheck=2 test go test -race=1 -v ./...
