// Databricks notebook source
// ==============================================================
// GOLD · RápidoEx Logística — KPIs de negocio
// ==============================================================
// Objetivo: a partir de la tabla Silver enriquecida, calcular los
// 5 KPIs de negocio (Paso 5 del examen) que respondan a las
// preguntas del Paso 1.
//
// Tablas Gold a construir:
//   1. gold.kpi_envios_diarios  → granularidad: día × ciudad
//   2. gold.kpi_repartidores    → granularidad: repartidor
//
// KPIs incluidos (cubren los 5 obligatorios + 1 extra):
//   1. Volumen        → total_envios, total_facturacion
//   2. Ratio          → pct_sla_cumplido
//   3. Temporal       → series por fecha
//   4. Ranking        → top repartidores
//   5. Extra          → tiempo medio de tránsito por ciudad

import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

// Configuración: nombres consistentes con Silver
val catalogo = "databricks_rapidoex"
val schemaSilver = "silver"
val schemaGold = "gold"

val tablaEnviosSilver = s"$catalogo.$schemaSilver.envios_enriquecidos"

val tablaKpiDiarios       = s"$catalogo.$schemaGold.kpi_envios_diarios"
val tablaKpiRepartidores  = s"$catalogo.$schemaGold.kpi_repartidores"

println(s"Origen:   $tablaEnviosSilver")
println(s"Gold 1:   $tablaKpiDiarios")
println(s"Gold 2:   $tablaKpiRepartidores")

// COMMAND ----------

// ==============================================================
// CELDA 2 — Lectura de Silver y enriquecimiento temporal
// ==============================================================
// Añadimos columnas derivadas de fecha que necesitaremos para todos
// los KPIs (granularidad temporal). Esto evita repetir el cálculo
// en cada agregación posterior.

val dfSilver = spark.table(tablaEnviosSilver)
  .withColumn("fecha_dia",  to_date(col("fecha_recogida")))
  .withColumn("anyo",       year(col("fecha_recogida")))
  .withColumn("mes",        month(col("fecha_recogida")))
  .withColumn("dia_semana", dayofweek(col("fecha_recogida")))  // 1=domingo
  .withColumn("hora",       hour(col("fecha_recogida")))
  .cache()  // Lo vamos a usar varias veces, lo cacheamos en memoria

val totalSilver = dfSilver.count()
println(s"📥 Silver enriquecidos: $totalSilver filas")

println("\n📋 Schema con dimensiones temporales:")
dfSilver.printSchema()

// COMMAND ----------

// ==============================================================
// CELDA 3 — Gold 1: KPIs diarios por ciudad
// ==============================================================
// Granularidad: una fila por día y ciudad.
// Esta tabla alimenta:
//   - Gráfico temporal (evolución diaria)
//   - Gráfico de comparación por ciudad
//   - KPI clave: % SLA cumplido
//
// KPIs incluidos:
//   - total_envios            (VOLUMEN — KPI obligatorio)
//   - envios_entregados, envios_incidencias, envios_devueltos
//   - pct_sla_cumplido        (RATIO — KPI obligatorio)
//   - pct_incidencia          (RATIO)
//   - tiempo_medio_transito_h (EXTRA — métrica operativa)
//   - facturacion_total       (VOLUMEN económico)

val dfKpiDiarios = dfSilver
  .groupBy("fecha_dia", "anyo", "mes", "ciudad")
  .agg(
    // KPIs de volumen
    count("tracking_id").alias("total_envios"),
    sum(when(col("estado") === "ENTREGADO", 1).otherwise(0)).alias("envios_entregados"),
    sum(when(col("estado") === "INCIDENCIA", 1).otherwise(0)).alias("envios_incidencias"),
    sum(when(col("estado") === "DEVUELTO", 1).otherwise(0)).alias("envios_devueltos"),

    // KPI de ratio: % SLA cumplido (entregas dentro de 24h sobre total de envíos)
    // entregado_a_tiempo ya viene como 0/1 desde Silver, así que sum() cuenta los OK
    round(
      sum(col("entregado_a_tiempo")) * 100.0 / count("tracking_id"),
      2
    ).alias("pct_sla_cumplido"),

    // KPI de ratio: % incidencias sobre total
    round(
      sum(when(col("estado") === "INCIDENCIA", 1).otherwise(0)) * 100.0 / count("tracking_id"),
      2
    ).alias("pct_incidencia"),

    // KPI operativo extra: tiempo medio de tránsito (solo entregados)
    round(avg(col("horas_transito")), 2).alias("tiempo_medio_transito_h"),

    // Facturación: suma del importe_envio
    round(sum("importe_envio"), 2).alias("facturacion_total")
  )
  .orderBy("fecha_dia", "ciudad")

// Vista previa para validar antes de guardar
println(s"📊 KPIs diarios calculados: ${dfKpiDiarios.count()} filas (día × ciudad)")
println("\n🔍 Muestra (primeros 10 días):")
display(dfKpiDiarios.limit(10))

// COMMAND ----------

// ==============================================================
// CELDA 4 — Gold 2: KPIs por repartidor con ranking
// ==============================================================
// Granularidad: una fila por repartidor.
// Esta tabla alimenta:
//   - Ranking top 10 (visualización tipo tabla)
//   - Comparativa por tipo de contrato
//   - Análisis de eficiencia por vehículo
//
// KPIs incluidos:
//   - envios_asignados (VOLUMEN)
//   - pct_sla_cumplido (RATIO)
//   - tiempo_medio_transito_h
//   - ranking_volumen (RANKING — KPI obligatorio)
//   - ranking_calidad
//   - score_combinado (KPI compuesto)

// Ventana global para calcular rankings sobre TODOS los repartidores
val ventanaVolumen = Window.orderBy(col("envios_asignados").desc)
val ventanaCalidad = Window.orderBy(col("pct_sla_cumplido").desc)

// Filtro: solo repartidores con al menos 10 envíos (excluimos los marginales
// que distorsionarían el ranking con ratios extremos sobre pocos datos)
val dfKpiRepartidores = dfSilver
  .groupBy(
    "repartidor_id",
    "repartidor_nombre",
    "repartidor_apellido",
    "tipo_contrato",
    "ciudad_asignada",
    "vehiculo"
  )
  .agg(
    count("tracking_id").alias("envios_asignados"),
    sum(when(col("estado") === "ENTREGADO", 1).otherwise(0)).alias("envios_entregados"),
    sum(when(col("estado") === "INCIDENCIA", 1).otherwise(0)).alias("incidencias"),

    // Ratio de calidad
    round(
      sum(col("entregado_a_tiempo")) * 100.0 / count("tracking_id"),
      2
    ).alias("pct_sla_cumplido"),

    // Productividad media (tiempo de tránsito)
    round(avg(col("horas_transito")), 2).alias("tiempo_medio_transito_h"),

    // Facturación generada por el repartidor
    round(sum("importe_envio"), 2).alias("facturacion_generada")
  )
  .filter(col("envios_asignados") >= 10)   // Filtro de mínimo razonable
  // KPI Ranking: añadimos posiciones (1, 2, 3...) según dos criterios distintos
  .withColumn("ranking_volumen", rank().over(ventanaVolumen))
  .withColumn("ranking_calidad", rank().over(ventanaCalidad))
  // Score combinado: media de los dos rankings (cuanto MENOR, mejor)
  .withColumn("score_combinado",
    round((col("ranking_volumen") + col("ranking_calidad")) / 2.0, 2)
  )
  .orderBy("score_combinado")

println(s"📊 KPIs de repartidores calculados: ${dfKpiRepartidores.count()} filas")
println("\n🏆 Top 10 mejor combinación de volumen y calidad (menor score = mejor):")
display(dfKpiRepartidores.limit(10))

// COMMAND ----------

// ==============================================================
// CELDA 5 — Persistir Gold como Delta + verificación
// ==============================================================
// Modo "overwrite": pipeline reproducible.

println("⏳ Escribiendo Gold 1: KPIs diarios por ciudad...")
dfKpiDiarios.write
  .format("delta")
  .mode("overwrite")
  .option("overwriteSchema", "true")
  .saveAsTable(tablaKpiDiarios)
println(s"✅ $tablaKpiDiarios")

println("\n⏳ Escribiendo Gold 2: KPIs de repartidores...")
dfKpiRepartidores.write
  .format("delta")
  .mode("overwrite")
  .option("overwriteSchema", "true")
  .saveAsTable(tablaKpiRepartidores)
println(s"✅ $tablaKpiRepartidores")

// Verificación
println("\n📊 Tablas Gold registradas:")
spark.sql(s"SHOW TABLES IN $catalogo.$schemaGold").show(false)

// COMMAND ----------

// ==============================================================
// CELDA 6 — Resumen ejecutivo de los KPIs (para el informe)
// ==============================================================
// Esta salida será el material directo del "Catálogo de KPIs" del
// informe técnico (Paso 8, sección 39 del examen).

import org.apache.spark.sql.Row

// Recuperamos los valores agregados a nivel global para mostrar el "headline"
val resumenGlobal = dfSilver.agg(
  count("tracking_id").alias("total"),
  sum(when(col("estado") === "ENTREGADO", 1).otherwise(0)).alias("entregados"),
  sum(when(col("estado") === "INCIDENCIA", 1).otherwise(0)).alias("incidencias"),
  sum(when(col("estado") === "DEVUELTO", 1).otherwise(0)).alias("devueltos"),
  round(sum(col("entregado_a_tiempo")) * 100.0 / count("tracking_id"), 2).alias("sla"),
  round(avg(col("horas_transito")), 2).alias("transito_h"),
  round(sum("importe_envio"), 2).alias("facturacion")
).collect()(0)

val total       = resumenGlobal.getAs[Long]("total")
val entregados  = resumenGlobal.getAs[Long]("entregados")
val incidencias = resumenGlobal.getAs[Long]("incidencias")
val devueltos   = resumenGlobal.getAs[Long]("devueltos")
val sla         = resumenGlobal.getAs[Double]("sla")
val transito    = resumenGlobal.getAs[Double]("transito_h")
val facturacion = resumenGlobal.getAs[Double]("facturacion")

// Top 3 ciudades por volumen
val topCiudades = dfSilver.groupBy("ciudad").count()
  .orderBy(col("count").desc).limit(3).collect()

// Top 3 repartidores por score combinado
val topRepartidores = dfKpiRepartidores
  .select("repartidor_nombre", "repartidor_apellido", "ciudad_asignada",
          "envios_asignados", "pct_sla_cumplido", "score_combinado")
  .limit(3).collect()

println("=" * 75)
println("RESUMEN EJECUTIVO — RápidoEx Logística 2024")
println("=" * 75)
println("")
println("📦 VOLUMEN OPERATIVO (año 2024)")
println(f"   Envíos totales procesados:        $total%,d")
println(f"   Entregados:                       $entregados%,d  (${entregados * 100.0 / total}%.1f%%)")
println(f"   Incidencias:                      $incidencias%,d  (${incidencias * 100.0 / total}%.1f%%)")
println(f"   Devueltos:                        $devueltos%,d  (${devueltos * 100.0 / total}%.1f%%)")
println("")
println("🎯 CALIDAD DE SERVICIO")
println(f"   %% SLA cumplido (entregas < 24h):  $sla%.2f%%")
println(f"   Tiempo medio de tránsito:         $transito%.2f horas")
println("")
println("💰 RESULTADO ECONÓMICO")
println(f"   Facturación total estimada:       EUR $facturacion%,.2f")
println("")
println("🏙️  TOP 3 CIUDADES POR VOLUMEN")
topCiudades.foreach(r =>
  println(f"   ${r.getString(0)}%-12s ${r.getLong(1)}%,d envíos")
)
println("")
println("🏆 TOP 3 REPARTIDORES (mejor combinación volumen + calidad)")
topRepartidores.foreach(r =>
  println(f"   ${r.getString(0)} ${r.getString(1)}  (${r.getString(2)}): " +
          f"${r.getLong(3)} envíos, ${r.getDouble(4)}%.1f%% SLA, " +
          f"score ${r.getDouble(5)}%.1f")
)
println("=" * 75)