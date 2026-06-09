object Dictionary {

  /**
   * Load entities from a dictionary file and create instances of the specified type.
   * @param filePath path to dictionary file (e.g., "data/people.txt")
   * @param entityType type of entity to create ("Person", "University", etc.)
   * @return Option containing list of entities, None if file missing
   */
  def loadFromFile(filePath: String, entityType: String): Option[List[NamedEntity]] = {
    FileIO.readDictionaryFile(filePath).map { lines =>
      lines.map { name =>
        entityType match {
          case "Person"              => new Person(name)
          case "Organization"        => new Organization(name)
          case "University"          => new University(name)
          case "Place"               => new Place(name)
          case "Technology"          => new Technology(name)
          case "ProgrammingLanguage" => new ProgrammingLanguage(name)
          case _                     => new Person(name) // fallback
        }
      }
    }
  }

  /**
   * Load all dictionary files and combine into a single list.
   * Prints warnings for missing dictionary files and continues with others.
   * @param entitiesDir path to directory containing entity files
   * @return combined list of all entities from all successfully loaded dictionaries
   */
  def loadAll(entitiesDir: String): List[NamedEntity] = {
    def loadOrWarn(fileName: String, entityType: String): List[NamedEntity] = {
      loadFromFile(s"$entitiesDir/$fileName", entityType).getOrElse {
        println(s"Warning: Could not load $entitiesDir/$fileName")
        List()
      }
    }

    loadOrWarn("people.txt", "Person") :::
      loadOrWarn("universities.txt", "University") :::
      loadOrWarn("languages.txt", "ProgrammingLanguage") :::
      loadOrWarn("organizations.txt", "Organization") :::
      loadOrWarn("places.txt", "Place")
  }
}