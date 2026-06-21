// Databricks notebook source
// Verificación de entorno: confirmamos Scala 2.13 y Spark 3.5.2
// Esta celda debe ejecutarse sin errores antes de continuar

println(s"Spark version:  ${spark.version}")
println(s"Scala version:  ${scala.util.Properties.versionString}")
println("Delta Lake:     integrado nativamente en Runtime 16.4 LTS")

// COMMAND ----------

// ==============================================================
// Configuración del proyecto RápidoEx Logística
// ==============================================================
// Centralizamos las rutas y nombres en variables para no repetir
// strings por todo el notebook. Esto facilita el mantenimiento
// y la reutilización del código en otros notebooks.

val catalogo = "databricks_rapidoex"
val schemaBronze = "bronze"

// Ruta del volume donde están los ficheros crudos
val rutaRaw = s"/Volumes/$catalogo/$schemaBronze/raw_data"

// Rutas concretas de cada fuente de datos
val rutaEnviosCsv         = s"$rutaRaw/envios.csv"
val rutaRepartidoresJson  = s"$rutaRaw/repartidores.json"

// Nombres de las tablas Delta de destino
val tablaEnviosBronze        = s"$catalogo.$schemaBronze.envios"
val tablaRepartidoresBronze  = s"$catalogo.$schemaBronze.repartidores"

println("Configuración cargada:")
println(s"  Catálogo:        $catalogo")
println(s"  Schema:          $schemaBronze")
println(s"  Ruta raw:        $rutaRaw")
println(s"  Tabla envíos:    $tablaEnviosBronze")
println(s"  Tabla rep.:      $tablaRepartidoresBronze")

// COMMAND ----------

// Listamos los ficheros del volume para confirmar que están accesibles
// dbutils es una utilidad propia de Databricks para operaciones de FS

println("Ficheros disponibles en el volume raw_data:")
display(dbutils.fs.ls(rutaRaw))

// COMMAND ----------

// ==============================================================
// CELDA 4 — Ingesta de envios.csv a la capa Bronze
// ==============================================================
// Filosofía: leemos los datos TAL CUAL vienen. Sin transformaciones,
// sin casts, sin filtros. Inferimos el schema solo para que Spark
// detecte los nombres de columna del header, pero usamos inferSchema=false
// para mantener todo como String y preservar los datos sucios para que
// la capa Silver los pueda tratar.

import org.apache.spark.sql.functions._

val dfEnviosBronze = spark.read
  .option("header", "true")          // El CSV tiene cabecera
  .option("inferSchema", "false")    // NO inferimos tipos: queremos los datos crudos
  .option("encoding", "UTF-8")       // Codificación estándar
  .csv(rutaEnviosCsv)
  // Añadimos columnas de auditoría: cuándo se ingirió y de qué fichero viene
  .withColumn("ingestion_timestamp", current_timestamp())
  .withColumn("source_file", lit("envios.csv"))
  .withColumn("source_system", lit("sistema_tracking_v1"))

// Información sobre lo ingerido
val totalEnvios = dfEnviosBronze.count()
println(s"✅ Envíos leídos de CSV: $totalEnvios filas")
println("\n📋 Schema resultante:")
dfEnviosBronze.printSchema()

println("\n🔍 Muestra de los primeros 5 registros:")
display(dfEnviosBronze.limit(5))

// COMMAND ----------

// ==============================================================
// CELDA 5 — Ingesta de repartidores.json a la capa Bronze
// ==============================================================
// JSON Lines: una línea por registro. Spark lo lee nativamente.
// Para JSON sí dejamos que Spark infiera el schema porque es seguro:
// JSON ya tiene tipado básico (strings, números) explícito en su sintaxis.

val dfRepartidoresBronze = spark.read
  .option("multiLine", "false")      // JSON Lines, no JSON array
  .json(rutaRepartidoresJson)
  // Columnas de auditoría: cuándo se ingirió y de qué fichero viene
  .withColumn("ingestion_timestamp", current_timestamp())
  .withColumn("source_file", lit("repartidores.json"))
  .withColumn("source_system", lit("sistema_rrhh_v2"))

// Información sobre lo ingerido
val totalRepartidores = dfRepartidoresBronze.count()
println(s"✅ Repartidores leídos de JSON: $totalRepartidores registros")
println("\n📋 Schema resultante:")
dfRepartidoresBronze.printSchema()

println("\n🔍 Muestra de los primeros 5 registros:")
display(dfRepartidoresBronze.limit(5))

// COMMAND ----------

// ==============================================================
// CELDA 6 — Persistir Bronze como tablas Delta en Unity Catalog
// ==============================================================
// Modo elegido: "overwrite"
//
// ¿Por qué overwrite y no append?
// En Bronze podríamos pensar en append (acumular datos cada ingesta).
// Pero para este proyecto académico, donde regeneramos el pipeline
// completo cada vez que ejecutamos los notebooks, "overwrite" tiene
// más sentido: garantiza un estado limpio y reproducible.
// En producción real, lo correcto sería append + idempotencia con
// merge para evitar duplicar entre ejecuciones.

println("⏳ Escribiendo tabla Bronze: envíos...")
dfEnviosBronze.write
  .format("delta")
  .mode("overwrite")
  .option("overwriteSchema", "true")   // Permite cambiar schema entre ejecuciones
  .saveAsTable(tablaEnviosBronze)
println(s"✅ Tabla creada: $tablaEnviosBronze")

println("\n⏳ Escribiendo tabla Bronze: repartidores...")
dfRepartidoresBronze.write
  .format("delta")
  .mode("overwrite")
  .option("overwriteSchema", "true")
  .saveAsTable(tablaRepartidoresBronze)
println(s"✅ Tabla creada: $tablaRepartidoresBronze")

// COMMAND ----------

// ==============================================================
// CELDA 7 — Verificación final del Bronze
// ==============================================================
// Comprobamos que las tablas existen en el metastore de Unity Catalog
// y mostramos un resumen para el informe.

println("📊 Tablas registradas en Unity Catalog:")
spark.sql(s"SHOW TABLES IN $catalogo.$schemaBronze").show(false)

println("\n📋 Conteo por tabla Bronze:")
val cntEnvios = spark.table(tablaEnviosBronze).count()
val cntRepartidores = spark.table(tablaRepartidoresBronze).count()

println(s"   $tablaEnviosBronze: $cntEnvios filas")
println(s"   $tablaRepartidoresBronze: $cntRepartidores filas")

println("\n📄 Detalles de la tabla envíos (formato Delta confirmado):")
spark.sql(s"DESCRIBE DETAIL $tablaEnviosBronze")
  .select("format", "name", "numFiles", "sizeInBytes")
  .show(false)

// COMMAND ----------

// ==============================================================
// CELDA 8 — Resumen ejecutivo de la capa Bronze
// ==============================================================
// Este resumen es la "evidencia" de cierre del notebook Bronze.
// Lo usamos para capturar en el informe técnico (Paso 8 del examen).

println("=" * 70)
println("RESUMEN DE INGESTA BRONZE — RápidoEx Logística")
println("=" * 70)
println(s"  Fecha de ejecución: ${java.time.LocalDateTime.now()}")
println(s"  Entorno: Azure Databricks · Runtime 16.4 LTS · Scala 2.13")
println("")
println("  Fuentes ingeridas:")
println(s"    • envios.csv          → $tablaEnviosBronze        ($cntEnvios filas)")
println(s"    • repartidores.json   → $tablaRepartidoresBronze  ($cntRepartidores registros)")
println("")
println("  Columnas de auditoría añadidas a cada tabla:")
println("    • ingestion_timestamp (TIMESTAMP)")
println("    • source_file         (STRING)")
println("    • source_system       (STRING)")
println("")
println("  Transformaciones de negocio aplicadas: NINGUNA (capa Bronze)")
println("  Siguiente paso: capa Silver — limpieza y normalización")
println("=" * 70)