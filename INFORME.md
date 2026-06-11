# Ejercicio 1 — Arquitectura del pipeline distribuido

**a) Arquitectura y flujo de datos del pipeline:**

A continuación se presenta el diagrama general del pipeline de procesamiento diseñado, ilustrando el flujo de los datos desde el origen en el Driver, su paso por el clúster (Workers/Shuffle) y el retorno de los resultados finales:

![Diagrama del Pipeline](img/diagrama.png)

Este diagrama se traduce en las siguientes transformaciones y operaciones dentro de Spark. En la siguiente tabla se detalla el tipo en Scala de cada conexión, la operación aplicada y dónde se ejecuta:

| De → A | Operación | Tipo de la conexión | Dónde corre |
|--------|-----------|---------------------|-------------|
| Archivo → suscripciones | lectura del JSON | `List[Subscription]` | Driver |
| Suscripciones → RDD | `parallelize` | `RDD[Subscription]` | Driver → cluster |
| Descarga de feeds | `flatMap` | `RDD[Post]` | Workers |
| Filtrado de vacíos | `filter` | `RDD[Post]` | Workers |
| Extracción + clasificación | `flatMap` | `RDD[NamedEntity]` | Workers |
| Armado del par clave-valor | `map` | `RDD[((String, String), Int)]` | Workers |
| Conteo por entidad | `reduceByKey` | `RDD[((String, String), Int)]` | Shuffle (barrera) |
| Ranking | `sortBy` | `RDD[((String, String), Int)]` | Shuffle (barrera) |
| Impresión | acción (`collect`/`foreach`) | `Array[((String, String), Int)]` | Driver |

*(Nota: La clave `(String, String)` representa el par `(tipo, nombre)` de la entidad y el `Int` es su conteo).*

Vale la pena señalar dos cosas clave del grafo: 
1. **El lugar de ejecución:** Los extremos corren en el driver y el cuerpo en los workers. La lectura del archivo produce una `List[Subscription]` que vive en el driver y recién se vuelve distribuida al pasar por `parallelize`. Del otro lado, la impresión es una acción que vuelve a concentrar el resultado en el driver.
2. **Mutación de tipos:** El tipo del dato va mutando a lo largo del pipeline (`Subscription` → `Post` → `NamedEntity` → par `((tipo, nombre), conteo)`), lo que deja en claro que cada etapa es una transformación de tipo, no una simple repetición del mismo dato.

---
**b)** Para cada paso del pipeline, determinen si puede expresarse como una de las abstracciones de Spark:

* **Conexión:** no lo podemos abstraer con estas transformaciones ya que esto lo ejecuta el Driver, no los workers. No se transforman datos, sino que se prepara el entorno y se establece el origen de la información.
* **Descarga:** lo podemos abstraer con **flatmap**. Cada tarea es independiente. Cada elemento de entrada (ej una suscripción/URL) puede producir una cantidad variable de resultados. Con flatmap aplanamos estas colecciones de distintas longitudes, y devolvemos directamente un [RDDPost] limpio.
* **Extracción y clasificación de entidades:** lo podemos abstraer todo junto con **flatMap**. Cada tarea es independiente y transforma cada elemento (el documento crudo) en una cantidad variable de resultados (cero, una o múltiples entidades ya procesadas y clasificadas).
* **Conteo:** lo podemos abstraer con **reduceByKey**. Como las entidades quedaron desparramadas por los distintos workers, necesitamos agrupar las que son idénticas para poder sumar sus apariciones. El total de una entidad depende de combinar la información de todos los elementos.
* **Ranking:** no lo podemos abstraer con estas transformaciones. Para ordenar los resultados de mayor a menor, Spark necesita ver todo completo. No alcanza con el trabajo aislado de cada worker, hay que juntar y cruzar toda la información del cluster para hacer un ordenamiento global, o mandarle todo al Driver para que arme el listado definitivo.

---
**c) Pasos del pipeline que son barreras:** 
 
* **Conteo:** Como una misma entidad pudo haber sido encontrada por varios workers distintos al mismo tiempo, ninguno puede dar el número total y definitivo por su cuenta. Todos tienen que frenar, avisar que terminaron su parte y empezar a cruzar la información para sumar las apariciones que quedaron desparramadas.
* **Ranking:** Para poder ordenar los resultados de mayor a menor, sí o sí necesitamos tener los números finales. No podemos ordenar nada globalmente hasta que absolutamente todos los workers hayan terminado el conteo y entregado sus resultados para ver bien todo completo.

**Pasos del pipeline que pueden ejecutarse independientemente:**

* **Descarga:** Cada worker agarra su parte de las URLs y baja el contenido por su cuenta. No necesitan coordinar absolutamente nada con los demás para poder avanzar.
* **Extracción y clasificación:** Cada worker toma los documentos que acaba de descargar, saca las entidades y las clasifica. Si un worker va más rápido que otro, sigue procesando sus elementos sin importarle que hace el resto. Como operan sobre cada documento por separado, acá no tenemos ninguna dependencia ni tiempos de espera.

---
**d) Restricciones que impone Spark sobre las funciones:**

* **Serialización (Todo tiene que poder viajar por la red):** El Driver agarra la función que escribimos (y todo su entorno capturado), la empaqueta y se la manda por la red a todos los workers. Por lo tanto, todo lo que esté adentro de esa función (variables, objetos, clases) tiene que ser "serializable" (convertible a bytes). Si le pasamos algo que no se puede empaquetar, como una conexión abierta a una base de datos o a un archivo local, Spark va a frenar todo y tirar un error `NotSerializableException` antes de arrancar, porque no sabe cómo mandarlo por el cable.
* **Estado compartido (Los workers son islas aisladas):** Cada worker ejecuta una copia de nuestra función y trabaja con su propia memoria aislada. Si intentamos modificar una variable global normal desde adentro de un map (ej un contador), cada worker va a sumar en su propia copia local, y el Driver nunca se va a enterar del resultado real. Para agregar o compartir información de forma segura entre todos, Spark nos obliga a usar sus abstracciones nativas (como `reduceByKey`) o herramientas específicas diseñadas para esto (como los Acumuladores o las variables Broadcast).
* **Efectos secundarios (No dejes rastros afuera):** Nuestras funciones tienen que ser "puras": agarran un dato, lo transforman y devuelven el resultado, sin alterar nada del mundo exterior. Como Spark está diseñado para tolerar fallos, si un worker se clava en la mitad del trabajo, Spark reinicia esa misma tarea en otro lado. Si nuestra función tenía el efecto secundario de insertar un registro, generaríamos inconsistencias o terminaríamos duplicando datos sin querer.

# Ejercicio 2 — Paralelizar la descarga de feeds

Decisiones de diseño:

* **Carga de suscripciones en el driver:** Usamos FileIO.readSubscriptions (que devuelve un Either para atajar errores de formato o archivo no encontrado) antes de paralelizar. Esto asegura que si el JSON inicial está roto o no existe, el programa corta de una sin levantar los workers al vicio.

* **flatMap para la descarga:** Usamos flatMap sobre el RDD de suscripciones porque cada URL nos puede dar N posts, o darnos cero si se cae la red o el parseo. Es la operación ideal para aplanar todo en un solo RDD[Post].

* **Manejo de fallos con Option/List.empty:** Dentro del flatMap, atajamos el resultado de downloadFeed con pattern matching. Si falla (devuelve None), imprimimos el warning y retornamos un List.empty[Post]. De esta forma aislamos el error y el pipeline sigue procesando los demás feeds.

* **Estadísticas mediante múltiples acciones y RDD auxiliares:** Para no romper la pureza de las transformaciones y evitar Accumulators (que pueden tener comportamientos raros si hay reintentos de tasks), armamos un downloadStatusRDD que simplemente mapea si la descarga y el parseo de cada feed fueron exitosos (Boolean, Boolean). Sobre ese RDD y el de posts filtramos y tiramos acciones como .count() y .mean() para calcular todas las métricas que pide la consigna.

---
## Pregunta: ¿qué pasaría si dejáramos propagar la excepción?

Si la función que se le pasa al flatMap no captura la excepción y la deja propagar,
falla la *tarea* (task) que estaba procesando esa partición. Spark reintenta esa tarea
varias veces (4 por defecto); si sigue fallando, **aborta el stage y, en consecuencia, el
job entero**. El resultado es que **un solo feed inválido tira abajo toda la descarga**:
se pierden los posts de todos los demás feeds, incluso los que se habían descargado
bien, porque el RDD nunca termina de materializarse.

Capturando la excepción dentro de la función y devolviendo List.empty[Post], el fallo queda acotado a ese elemento: ese feed aporta cero posts, el fallo se contabiliza luego mediante nuestro RDD de estado (downloadStatusRDD) y el resto del pipeline continúa normalmente. Es decir, transformamos un fallo fatal para el job en un fallo local y observable, que es justo lo que se espera de un sistema distribuido tolerante a fallos.

# Ejercicio 3 — Paralelizar el cómputo de entidades nombradas

Decisiones de diseño:

* **Broadcast del diccionario:** Cargamos el diccionario en el driver con `Dictionary.loadAll` y lo distribuimos con `sc.broadcast`. Si lo capturáramos directo en el closure del `flatMap`, Spark lo serializaría y mandaría con cada tarea individual. Con broadcast se envía una sola copia por worker y todos los tasks la reutilizan desde memoria local.

* **Pipeline encadenado sin collect intermedios:** Las tres transformaciones (`flatMap` → `map` → `reduceByKey`) se encadenan sin materializar el RDD entre etapas. El `collect()` se hace solo al final, cuando ya tenemos los conteos agregados, trayendo al driver muchos menos datos que si hubiéramos traído todos los posts o todas las entidades crudas.

* **typeStats calculado desde el mapa reducido:** Una vez que `reduceByKey` devuelve el mapa `((tipo, nombre) → count)`, los stats por tipo los calculamos agrupando ese mapa en el driver, sin ninguna acción adicional sobre el cluster.

---

## Pregunta: `reduceByKey` es una barrera de sincronización. ¿Qué ocurre en el cluster en ese punto? ¿Por qué es inevitable?

`reduceByKey` dispara un *shuffle*: Spark redistribuye todos los pares por la red para que cada clave `(tipo, nombre)` quede concentrada en un único nodo, que recién ahí puede sumar los 1s y dar el conteo final. Es inevitable porque el conteo de una entidad depende de cuántas veces apareció en cualquier post de cualquier worker; no hay forma de saber el total sin cruzar información de todos los nodos.

## Pregunta: ¿Qué restricciones debe cumplir la función que se le pasa a `reduceByKey`?

Tiene que ser **asociativa** y **conmutativa**. Spark puede combinar subtotales en cualquier orden y en múltiples pasadas (primero dentro del mismo worker, luego entre workers), así que la función tiene que dar el mismo resultado sin importar ese orden. La suma entera `(_ + _)` cumple ambas. Una diferencia `(_ - _)` no y daría resultados incorrectos.

## Pregunta: ¿Dónde se hace la lectura del diccionario? ¿En el driver o los workers?

En el **driver**, antes de cualquier transformación distribuida. Si se leyera dentro del `flatMap`, los workers necesitarían acceder al filesystem local del driver (imposible en un cluster real) y además lo leerían N veces innecesariamente, una por partición.
