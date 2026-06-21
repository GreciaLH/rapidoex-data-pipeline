# 🚚 RápidoEx — Pipeline de Datos End-to-End
### ETL con Arquitectura Medallion sobre Azure Databricks

![Scala](https://img.shields.io/badge/Scala-2.13-DC322F?style=flat&logo=scala&logoColor=white)
![Apache Spark](https://img.shields.io/badge/Apache%20Spark-3.5.2-E25A1C?style=flat&logo=apachespark&logoColor=white)
![Delta Lake](https://img.shields.io/badge/Delta%20Lake-3.x-00ADD8?style=flat)
![Azure Databricks](https://img.shields.io/badge/Azure%20Databricks-Premium-0078D4?style=flat&logo=microsoftazure&logoColor=white)
![Power BI](https://img.shields.io/badge/Power%20BI-Desktop-F2C811?style=flat&logo=powerbi&logoColor=black)
![Python](https://img.shields.io/badge/Python-3.x-3776AB?style=flat&logo=python&logoColor=white)

---

## 📋 Descripción del proyecto

Este proyecto implementa un **pipeline de datos completo de extremo a extremo** para una empresa ficticia de logística de última milla, **RápidoEx Logística S.L.**, que procesa envíos urbanos en 5 ciudades españolas.

El pipeline cubre el ciclo de vida completo de la ingeniería de datos:
- **Generación de datos sintéticos** con problemas de calidad realistas
- **Arquitectura Medallion** (Bronze → Silver → Gold) sobre Delta Lake
- **Resolución documentada de 12 problemas de calidad de datos**
- **Cálculo de KPIs de negocio** con agregaciones Spark y funciones Window
- **Dashboard ejecutivo** en Power BI Desktop

Desarrollado como proyecto final del curso de certificación **Procesamiento Big Data con Scala (IFCD0115)**.

---

## 🏗️ Arquitectura

```
┌─────────────────────────────────────────────────────────────┐
│                    FUENTES DE ORIGEN                         │
│  envios.csv (15.300 filas)    repartidores.json (253 reg.)  │
│  Sistema de tracking          Sistema de RRHH               │
└──────────────┬────────────────────────┬─────────────────────┘
               │                        │
               ▼                        ▼
┌─────────────────────────────────────────────────────────────┐
│                   CAPA BRONZE (Raw)                          │
│   bronze.envios  +  bronze.repartidores                     │
│   Delta Lake · Sin transformaciones · Columnas de auditoría │
└──────────────────────────┬──────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────┐
│                   CAPA SILVER (Limpia)                       │
│   silver.envios_enriquecidos + silver.repartidores          │
│   12 problemas de calidad resueltos · JOIN entre fuentes    │
└──────────────────────────┬──────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────┐
│                    CAPA GOLD (KPIs)                          │
│   gold.kpi_envios_diarios   gold.kpi_repartidores           │
│   5 KPIs de negocio · Funciones Window · Ranking            │
└──────────────────────────┬──────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────┐
│             DASHBOARD POWER BI DESKTOP                       │
│   4 visualizaciones · 2 segmentadores · Análisis SLA        │
└─────────────────────────────────────────────────────────────┘
```

---

## 🛠️ Tecnologías utilizadas

| Capa | Tecnología |
|---|---|
| Plataforma cloud | Azure Databricks Premium (Hybrid) · Sweden Central |
| Procesamiento | Apache Spark 3.5.2 + Scala 2.13 |
| Almacenamiento | Delta Lake sobre Unity Catalog |
| Cluster | Single Node · Standard_D4s_v3 · Runtime 16.4 LTS |
| Visualización | Power BI Desktop |
| Generación de datos | Python 3 + Faker |

---

## 📊 KPIs de negocio

El pipeline responde a 4 preguntas de negocio mediante 5 KPIs:

| KPI | Tipo | Valor | Pregunta de negocio |
|---|---|---|---|
| Total de envíos procesados | Volumen | 14.783 | Escala operativa anual |
| % SLA cumplido | Ratio | **51,92%** | Entregas a tiempo (<24h) |
| Evolución mensual del SLA por ciudad | Temporal | 12 meses × 5 ciudades | Patrones estacionales |
| Top 10 repartidores | Ranking | Score: volumen + calidad | Rendimiento individual |
| Tiempo medio de tránsito | Operativo | 20,45 horas | Eficiencia del proceso |

**Hallazgo clave**: aunque el 85% de los envíos se entrega correctamente, solo el 52% cumple el SLA de 24 horas. RápidoEx tiene un **problema de velocidad, no de cobertura** — el principal hallazgo operativo del análisis.

---

## 🧹 Calidad de datos

Se introdujeron y resolvieron 12 problemas de calidad de forma documentada:

| # | Problema | Estrategia | Registros afectados |
|---|---|---|---|
| 1 | Nulos en `peso_kg` | Imputar con mediana (12,22 kg) | 459 filas |
| 2 | Nulos en `fecha_entrega` | Mantener (legítimo: envíos no entregados) | 2.249 filas |
| 3 | Duplicados exactos | `dropDuplicates()` | -289 filas |
| 4 | Formatos de fecha mezclados (`yyyy-MM-dd` vs `dd/MM/yyyy`) | `coalesce(to_timestamp(...), to_timestamp(...))` | 1.530 filas |
| 5 | Inconsistencias en ciudad (mayúsculas, espacios) | `initcap(trim(col))` | 1.224 filas |
| 6 | Códigos postales con menos de 5 dígitos | `lpad(col, 5, "0")` | 156 filas |
| 7 | Pesos con coma decimal española (`"2,4"` → `2.4`) | `regexp_replace` + `cast(DoubleType)` | 199 filas |
| 8 | Pesos fuera de rango (negativos o > 100 kg) | Filtro: `peso_kg > 0.1 AND ≤ 50` | -76 filas |
| 9 | Foreign key rota en `repartidor_id` | Join `left_anti` + descarte | -152 filas |
| 10 | Nulos en `telefono` | Mantener (no crítico para KPIs) | 25 registros |
| 11 | Repartidores duplicados | `dropDuplicates("repartidor_id")` | -3 registros |
| 12 | Inconsistencias en `tipo_contrato` (`EMP`, `empleado`…) | `when(upper(...).isin(...))` | 15 registros |

**Tasa de aprovechamiento Bronze → Silver: 96,62%** (14.783 de 15.300 registros)

---

## 📁 Estructura del repositorio

```
rapidoex-data-pipeline/
├── datos/
│   ├── generar_datos.py           ← Script de generación de datos sintéticos
│   ├── envios.csv                 ← Fuente principal (15.300 filas, CSV)
│   └── repartidores.json          ← Fuente secundaria (253 registros, JSON Lines)
├── etl/
│   ├── etl_bronze_rapidoex.scala  ← Capa Bronze: ingesta raw
│   └── etl_silver_rapidoex.scala  ← Capa Silver: limpieza + JOIN
├── analisis/
│   ├── etl_gold_rapidoex.scala    ← Capa Gold: KPIs y agregaciones
│   └── export_powerbi.scala       ← Exportación CSV para Power BI
├── informe/
│   ├── informe_proyecto.pdf       ← Informe técnico completo (8 secciones)
│   └── dashboard.pdf              ← Dashboard exportado a PDF
├── dashboard/
│   └── Dashboard_RapidoEx.pbix   ← Fichero Power BI Desktop
└── README.md
```

---

## 🚀 Cómo reproducir el proyecto

### Requisitos previos
- Workspace de Azure Databricks (Runtime 16.4 LTS, Scala 2.13)
- Unity Catalog habilitado con un catálogo llamado `databricks_rapidoex`
- Power BI Desktop (gratuito)
- Python 3 + `faker` + `pandas` (para generación de datos)

### Pasos

**1. Generar los datos sintéticos**
```bash
pip install faker pandas
python datos/generar_datos.py
# Genera: envios.csv (15.300 filas) + repartidores.json (253 registros)
```

**2. Subir al Volume de Databricks**

Sube `envios.csv` y `repartidores.json` a:
```
/Volumes/databricks_rapidoex/bronze/raw_data/
```

**3. Ejecutar el pipeline (en orden)**
```
etl/etl_bronze_rapidoex.scala     → Crea bronze.envios + bronze.repartidores
etl/etl_silver_rapidoex.scala     → Crea silver.envios_enriquecidos
analisis/etl_gold_rapidoex.scala  → Crea gold.kpi_envios_diarios + gold.kpi_repartidores
analisis/export_powerbi.scala     → Exporta CSVs a /Volumes/.../gold/export_powerbi/
```

**4. Abrir el dashboard**

Abre `dashboard/Dashboard_RapidoEx.pbix` en Power BI Desktop y actualiza los datos desde los CSVs exportados.

---

## 💡 Competencias demostradas

- **Spark + Scala**: DataFrame API, funciones Window, `coalesce`, join `left_anti`, escritura Delta
- **Delta Lake**: transacciones ACID, `saveAsTable`, `overwriteSchema`, integración con Unity Catalog
- **Calidad de datos**: 12 problemas documentados con estrategias de resolución justificadas
- **Arquitectura Medallion**: Bronze (raw) → Silver (limpia + enriquecida) → Gold (agregada)
- **Azure Cloud**: workspace Databricks Premium, gestión de clusters, Unity Catalog Volumes
- **Business Intelligence**: Power BI Desktop, segmentadores, tarjetas KPI, gráficos temporales
- **Python**: generación de datos sintéticos con Faker y aleatoriedad controlada (semilla fija)

---

## 📈 Dashboard

El dashboard de Power BI incluye:

- 📊 **Gráfico de líneas**: evolución mensual del SLA por ciudad (enero–diciembre 2024)
- 📊 **Gráfico de barras**: total de envíos por ciudad
- 🃏 **Tarjeta KPI**: total de envíos procesados (14.783)
- 📋 **Tabla**: Top 10 repartidores ordenados por score de volumen + calidad
- 🔽 **Segmentadores**: filtro por ciudad · filtro por tipo de contrato

---

## 📄 Informe técnico

El informe técnico completo (`informe/informe_proyecto.pdf`) incluye:
1. Resumen ejecutivo
2. Dominio de negocio y preguntas
3. Arquitectura del pipeline
4. Descripción de las fuentes de datos
5. Decisiones de limpieza
6. Catálogo de KPIs
7. Capturas del dashboard
8. Conclusiones y mejoras futuras

---

## 👩‍💻 Autora

**Grecia L. Herrera**
- GitHub: [@GreciaLH](https://github.com/GreciaLH)

*Proyecto final del curso "Procesamiento Big Data con Scala" (IFCD0115) — Mayo 2026*
