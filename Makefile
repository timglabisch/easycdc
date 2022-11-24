start_mysql:
	docker run --rm \
		-e "MYSQL_ROOT_PASSWORD=password" \
		-e "MYSQL_ROOT_HOST=%" \
		-p "33069:3306" \
		--mount "type=bind,src=/$(CURDIR)/docker/mysql/my.cnf,dst=/etc/my.cnf" \
		mysql/mysql-server:latest

run:
	cargo run -- easycdc.toml

test:
	docker kill easycdc_integration_1; cargo test -- --test-threads=1