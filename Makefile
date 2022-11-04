start_mysql:
	docker run --rm \
		-e "MYSQL_ROOT_PASSWORD=password" \
		-e "MYSQL_ROOT_HOST=%" \
		-p "3306:3306" \
		--mount "type=bind,src=/$(CURDIR)/docker/mysql/my.cnf,dst=/etc/my.cnf" \
		mysql/mysql-server:latest