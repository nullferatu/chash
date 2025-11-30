# Makefile for PA#2 (Rust Implementation)
# Wraps cargo commands to produce a binary named 'chash'

CARGO = cargo
release:
	$(CARGO) build --release
	cp target/release/chash chash

clean:
	$(CARGO) clean
	rm -f chash
	rm -f hash.log