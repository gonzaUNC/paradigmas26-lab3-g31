**b)** Para cada paso del pipeline, determinen si puede expresarse como una de las abstracciones de Spark:

* **Conexión:** no lo podemos abstraer con estas transformaciones ya que esto lo ejecuta el Driver, no los workers. No se transforman datos, sino que se prepara el entorno y se establece el origen de la información.
* **Descarga:** lo podemos abstraer con **map**. Cada tarea es independiente y transforma cada elemento de entrada (ej URL) en exactamente un resultado (el documento o HTML descargado).
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
