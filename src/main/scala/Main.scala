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

    // EJERCICIO 2C - Calcular e imprimir estadisticas

    val downloadStatusRDD = subscriptionsRDD.map { subscription =>
      FileIO.downloadFeed(subscription.url) match {
        case Some(content) =>
          val posts = JsonParser.parsePosts(content, subscription)
          (true, posts.nonEmpty)  // (descarga OK, tuvo al menos un post)
        case None =>
          (false, false)           // descarga fallida
      }
    }

    val feedsSuccess = downloadStatusRDD.filter(_._1).count().toInt
    val feedsFailed  = downloadStatusRDD.filter(!_._1).count().toInt
    // postsFailed: suscripciones de las que no se obtuvo ningún post
    // (fallo de descarga O fallo de parseo)
    val postsFailed  = downloadStatusRDD.filter(!_._2).count().toInt

    val postsSuccess  = allPostsRDD.count().toInt
    val filteredCount = filteredPostsRDD.count().toInt
    val postsFiltered = postsSuccess - filteredCount
  
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
      "postsFailed"   -> postsFailed,
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
