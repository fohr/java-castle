.PHONY: build
build:
	ant jar

.PHONY: docs
docs:
	ant docs

.PHONY: clean
clean:
	ant clean

.PHONY: install
install:
	ant install
