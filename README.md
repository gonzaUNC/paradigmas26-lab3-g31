# Reddit Named Entity Recognition — Lab 3

Sistema de procesamiento distribuido con Apache Spark que descarga posts de Reddit, detecta entidades nombradas (personas, organizaciones, lugares, etc.) y muestra un ranking de las más frecuentes.

## Requisitos

- **Java 17** (requerido por Spark 3.x; versiones más nuevas como 24/25 son incompatibles)
- **sbt 1.9** o posterior
- **Scala 2.13** (descargado automáticamente por sbt)

## Instalación de Java 17

### Linux (Ubuntu/Debian)
```bash
sudo apt update && sudo apt install openjdk-17-jdk
```

### macOS
```bash
brew install openjdk@17
sudo ln -sfn /opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk \
             /Library/Java/JavaVirtualMachines/openjdk-17.jdk
```

No es necesario cambiar la versión global de Java del sistema; el Makefile detecta y usa Java 17 solo para este proyecto.

## Cómo ejecutar

### Conectando a Reddit real

```bash
make run
```

Descarga posts de los subreddits definidos en `data/valid_subscriptions.json`.

### Usando el servidor mock local (sin consultar Reddit)

Primero, descargar el mock desde el [link provisto por la cátedra](https://drive.google.com/file/d/1L0GKui_4FsxTGCQ2EAWllwRW23kHEQZI/view) y levantarlo en una terminal aparte:

```bash
sbt run
# Fake Reddit API running on http://localhost:8123
```

Luego, en otra terminal:

```bash
make run-local
```

Usa `data/local_subscriptions.json`, que apunta a `http://localhost:8123`.

### Comandos disponibles

| Comando | Descripción |
|---------|-------------|
| `make run` | Ejecuta con `data/valid_subscriptions.json` (Reddit real) |
| `make run-local` | Ejecuta con `data/local_subscriptions.json` (mock local) |
| `make compile` | Solo compila sin ejecutar |
| `make clean` | Limpia los artefactos de compilación |

Ambos `run` y `run-local` usan `data/valid_entities/` como diccionario y muestran el top 5 de entidades.

## Estructura del proyecto

```
.
├── build.sbt
├── Makefile
├── README.md
├── INFORME.md
├── data/
│   ├── valid_entities/
│   │   ├── people.txt
│   │   ├── organizations.txt
│   │   ├── universities.txt
│   │   ├── places.txt
│   │   └── languages.txt
│   ├── valid_subscriptions.json
│   └── local_subscriptions.json
└── src/main/scala/
    ├── Main.scala
    ├── FileIO.scala
    ├── JsonParser.scala
    ├── Analyzer.scala
    ├── Dictionary.scala
    ├── NamedEntity.scala
    ├── Formatters.scala
    ├── CommandLineArgs.scala
    ├── Subscription.scala
    └── Post.scala
```

## Solución de problemas comunes

**Error `getSubject is not supported` o `IllegalAccessError`:** Estás usando Java 24/25. El Makefile detecta Java 17 automáticamente; si falla, verificá que esté instalado con `java -version` luego de seleccionar la versión 17.

**`sbt` no encontrado:** Instalá sbt desde https://www.scala-sbt.org/download.html

**Spark UI:** Al correr en modo local, la Spark UI está disponible en http://localhost:4040 mientras el programa está ejecutándose.