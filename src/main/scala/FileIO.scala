import scala.io.Source
import org.json4s._
import org.json4s.jackson.JsonMethods._
import java.io.FileNotFoundException

object FileIO {

  /**
   * Read subscriptions from JSON file.
   * @param filePath path to subscriptions file
   * @return list of options: Some(Subscription) for valid entries, None for malformed entries
   * returns empty list if file not found
   */
  def readSubscriptions(filePath: String): Either[String, List[Option[Subscription]]] = {
    implicit val formats: Formats = DefaultFormats

    // Leer el archivo (puede no existir)
    val content = try {
      val source = Source.fromFile(filePath)
      val c = source.mkString
      source.close()
      c
    } catch {
      case _: FileNotFoundException =>
        return Left(s"Error: Could not load $filePath - file not found")
    }

    // Parsear JSON (puede no ser JSON válido)
    val json = try {
      parse(content)
    } catch {
      case _: Exception =>
        return Left(s"Error: Could not load $filePath - invalid JSON format")
    }

    // Extraer lista de mapas (puede no tener la estructura esperada)
    val subscriptionMaps = try {
      json.extract[List[Map[String, String]]]
    } catch {
      case _: Exception =>
        return Left(s"Error: Could not load $filePath - invalid JSON format")
    }

    // Convertir cada entrada: None si le falta "name" o "url"
    val subscriptions = subscriptionMaps.map { sub =>
      try {
        Some(Subscription(sub("name"), sub("url")))
      } catch {
        case _: NoSuchElementException =>
          println("Warning: Skipping malformed subscription (missing 'name' or 'url' field)")
          None
      }
    }

    Right(subscriptions)
  }

  /**
   * Download feed JSON from URL.
   * @param url Reddit feed URL
   * @return Option containing JSON as String, None on network error or timeout
   */
  def downloadFeed(url: String): Option[String] = {
    try {
      val source = Source.fromURL(url)
      val content = source.mkString
      source.close()
      Some(content)
    } catch {
      case _: Exception => None
    }
  }

  /**
   * Read dictionary file line by line.
   * @param filePath path to dictionary file
   * @return Option containing list of entities, None if file missing
   */
  def readDictionaryFile(filePath: String): Option[List[String]] = {
    try {
      val source = Source.fromFile(filePath)
      val lines = source.getLines()
        .map(_.trim)
        .filter(_.nonEmpty)
        .filterNot(_.startsWith("#"))
        .toList
      source.close()
      Some(lines)
    } catch {
      case _: Exception => None
    }
  }
}