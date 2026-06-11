import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]): Unit = {
    // Parse command-line arguments
    val cmdArgs = CommandLineArgs.parse(args) match {
      case Some(parsed) => parsed
      case None => return // scopt prints error messages
    }

    // creamos SparkSession localmente
    val spark = SparkSession.builder()
      .appName("RedditNER")
      .master("local[*]")
      .config("spark.driver.memory", "768m") // limitamos por la ram
      .config("spark.ui.enabled", "false")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    val sc = spark.sparkContext

    // EJERCICIO 2A - Leer suscripciones en el driver y paralelizarlas

    // Distribuimos las descargas de cada feed (una por suscripcion), que es lo costoso
    // Load subscriptions
    val subscriptionOpts = FileIO.readSubscriptions(cmdArgs.subscriptionFile) match {
      case Left(error) =>
        println(error)
        spark.stop()
        return
      case Right(opts) => opts
    }
    // Filter out malformed subscriptions (None values)
    val subscriptions = subscriptionOpts.flatten

    if (subscriptions.isEmpty) {
      println("Error: No valid subscriptions found")
      spark.stop()
      return
    }
    
    // Eliminar las entradas malformadas (las que readSubscriptions marcó None)
    val subscriptionsRDD = sc.parallelize(subscriptions)

    // EJERCICIO 4A - Definir Accumulators ANTES de lanzar el pipeline

    val accFeedsSuccess  = sc.longAccumulator("feedsSuccess")
    val accFeedsFailed   = sc.longAccumulator("feedsFailed")
    val accPostsTotal    = sc.longAccumulator("postsTotal")
    val accPostsFiltered = sc.longAccumulator("postsFiltered")

    // EJERCICIO 4C - Medir tiempo de la primera acción terminal (descarga + filtrado)
    val t0 = System.currentTimeMillis()

    // EJERCICIO 2B - flatMap sobre el RDD de suscripciones

    // Los errores se capturan DENTRO del flatMap para que un fallo no cancele
    // el procesamiento de las demás suscripciones.
    
    val allPostsRDD = subscriptionsRDD.flatMap { subscription =>
      FileIO.downloadFeed(subscription.url) match {
      case Some(content) =>
        val posts = JsonParser.parsePosts(content, subscription)
        if (posts.nonEmpty) {
          accFeedsSuccess.add(1)
          accPostsTotal.add(posts.length)
        } else {
          accFeedsFailed.add(1) // descarga OK pero sin posts
        }
        posts
      case None =>
        println(s"Warning: Failed to download from '${subscription.name}' (${subscription.url})")
        accFeedsFailed.add(1)
        List.empty[Post]
      }
    }

    // Filtrar posts donde title o selftext están vacíos
    // Se hace despues del flatMap para poder contar cuántos se filtraron
    val filteredPostsRDD = allPostsRDD.filter { post =>
      val keep = post.title.nonEmpty && post.selftext.nonEmpty && post.selftext.trim.nonEmpty
      if (!keep) accPostsFiltered.add(1)
      keep
    }

    // EJERCICIO 5B - cache() para evitar recomputar el pipeline (incluyendo descargas HTTP)
    // desde las acciones siguientes: .count(), .mean() y el flatMap de NER
    filteredPostsRDD.cache()

    // EJERCICIO 4B - Usamos los Accumulators para las estadísticas
    // .count() es la primera acción terminal: materializa allPostsRDD + filteredPostsRDD
    val filteredCount = filteredPostsRDD.count()

    val t1 = System.currentTimeMillis()
    println(s"[Tiempo] Descarga + filtrado: ${(t1 - t0) / 1000.0} s")

    // Ahora los Accumulators tienen sus valores definitivos (la acción ya terminó)
    val feedsSuccess  = accFeedsSuccess.value.toInt
    val feedsFailed   = accFeedsFailed.value.toInt
    val postsSuccess  = accPostsTotal.value.toInt
    val postsFiltered = accPostsFiltered.value.toInt
  
    // Prepare statistics
    
    val avgChars: Int =
      if (filteredCount == 0) 0
      else filteredPostsRDD
        .map(p => (p.title.length + p.selftext.length).toDouble)
        .mean()
        .toInt

    val stats = Map(
      "feedsSuccess"  -> feedsSuccess,
      "feedsFailed"   -> feedsFailed,
      "postsSuccess"  -> postsSuccess,
      "postsFailed"   -> 0,
      "postsFiltered" -> postsFiltered,
      "avgChars"      -> avgChars
    )
    // Print output
    println(Formatters.formatProcessingStats(stats))
    println()

    // EJERCICIO 2D - Salir si no hay posts válidos

    if (filteredCount == 0) {
      println("Error: No valid posts downloaded after filtering")
      spark.stop()
      return
    }

    if (!new java.io.File(cmdArgs.entitiesDir).exists()) {
      println(s"Error: entities directory '${cmdArgs.entitiesDir}' not found")
      spark.stop()
      return
    }

    // Load dictionaries
    val dictionary = Dictionary.loadAll(cmdArgs.entitiesDir)

    // EJERCICIO 3 - Pipeline Map-Reduce distribuido para NER
    val broadcastDict = sc.broadcast(dictionary)

    // EJERCICIO 4C - Medir tiempo de la segunda acción terminal (NER + reduce)
    val t2 = System.currentTimeMillis()

    // EJERCICIO 3A flatMap: por cada post, extraer sus entidades nombradas
    val entitiesRDD = filteredPostsRDD.flatMap { post =>
      val combinedText = post.title + " " + post.selftext
      Analyzer.detectEntities(combinedText, broadcastDict.value)
    }

    // EJERCICIO 3B map: convertir cada NamedEntity en ((tipo, nombre), 1)
    val entityPairsRDD = entitiesRDD.map { entity =>
      ((entity.entityType, entity.text), 1)
    }

    // EJERCICIO 3C reduceByKey: sumar los 1s para obtener el conteo total por entidad
    val entityCountsRDD = entityPairsRDD.reduceByKey(_ + _)

    val entityCountsMap: Map[(String, String), Int] = entityCountsRDD.collect().toMap

    val t3 = System.currentTimeMillis()
    println(s"[Tiempo] NER + reducción: ${(t3 - t2) / 1000.0} s")
    println(s"[Tiempo] Total pipeline: ${(t3 - t0) / 1000.0} s")
    println()

    // EJERCICIO 4A - Imprimir los cuatro Accumulators luego de la acción terminal final
    println("============ MÉTRICAS DE EJECUCIÓN (Accumulators) ============")
    println(s"Feeds descargados exitosamente : ${accFeedsSuccess.value}")
    println(s"Feeds fallidos                 : ${accFeedsFailed.value}")
    println(s"Posts descargados en total     : ${accPostsTotal.value}")
    println(s"Posts descartados (vacíos)     : ${accPostsFiltered.value}")
    println()

    val typeStats: Map[String, Int] = {
      val byType = entityCountsMap
        .groupBy { case ((entityType, _), _) => entityType }
        .view
        .mapValues(_.values.sum)
        .toMap
      byType + ("total" -> entityCountsMap.values.sum)
    }

    val entityCounts = entityCountsMap

    println(Formatters.formatTypeStats(typeStats))
    println()
    println(Formatters.formatEntityStats(entityCounts, cmdArgs.topK))

    spark.stop()
  }
}
