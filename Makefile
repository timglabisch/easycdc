start_mysql:
	cd easycdc; docker run --rm \
		-e "MYSQL_ROOT_PASSWORD=password" \
		-e "MYSQL_ROOT_HOST=%" \
		-p "33069:3306" \
		--mount "type=bind,src=/$(CURDIR)/docker/mysql/my.cnf,dst=/etc/my.cnf" \
		mysql/mysql-server:latest

run:
	cd easycdc; cargo run -- easycdc.toml

test:
	cd easycdc && docker kill easycdc_integration_1; cargo test -- --test-threads=1