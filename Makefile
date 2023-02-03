run:
	cd easycdc; cargo run -- easycdc.toml

run_release:
	cd easycdc; cargo run --release -- easycdc.toml

test:
	cd easycdc && docker kill easycdc_integration_1; cargo test -- --test-threads=1

run_benchmark:
	cd mysql_benchmark && cargo run --release

up:
	docker compose up -d