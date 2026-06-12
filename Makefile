JAVA_HOME_17 := $(shell \
	if [ "$$(uname)" = "Darwin" ]; then \
		/usr/libexec/java_home -v 17 2>/dev/null; \
	else \
		dirname $$(dirname $$(readlink -f $$(update-alternatives --list java 2>/dev/null | grep java-17 | head -1))); \
	fi)

SBT_OPTS := --add-exports=java.base/sun.nio.ch=ALL-UNNAMED

SBT_RUN = JAVA_HOME="$(JAVA_HOME_17)" PATH="$(JAVA_HOME_17)/bin:$(PATH)" SBT_OPTS="$(SBT_OPTS)" sbt

.PHONY: run run-local compile clean

## Ejecuta con suscripciones válidas (Reddit real)
run:
	$(SBT_RUN) "run --subscription-file data/valid_subscriptions.json --entities-dir data/valid_entities/ --top-k 5"

## Ejecuta con suscripciones locales (mock server en localhost:8123)
run-local:
	$(SBT_RUN) "run --subscription-file data/local_subscriptions.json --entities-dir data/valid_entities/ --top-k 5"

## Solo compila
compile:
	$(SBT_RUN) compile

## Limpia los artefactos de compilación
clean:
	sbt clean
