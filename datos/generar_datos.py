"""
================================================================================
Script de generación de datos sintéticos - RápidoEx Logística S.L.
================================================================================

Proyecto Final - Procesamiento Big Data con Scala (IFCD0115)
Autor: [Tu Nombre]
Fecha: 2026

Este script genera dos ficheros de datos sintéticos que simulan las dos fuentes
de origen del pipeline ETL:

    1) envios.csv         - ~15.000 filas, sistema de tracking de envíos
    2) repartidores.json  -   250 filas, sistema de RRHH

Los datos contienen 12 problemas de calidad INTENCIONALES que se documentan en
cada bloque y se resolverán en la capa Silver del pipeline.

Requisitos:
    pip install faker pandas

Uso:
    python generar_datos.py

Salidas:
    ./envios.csv
    ./repartidores.json
"""

import csv
import json
import random
from datetime import datetime, timedelta

import pandas as pd
from faker import Faker

# ------------------------------------------------------------------------------
# Configuración global
# ------------------------------------------------------------------------------
# Fijamos semilla para que los datos sean reproducibles entre ejecuciones.
# Esto es importante para el examen: si el evaluador regenera los datos,
# obtendrá EXACTAMENTE los mismos resultados.
SEMILLA = 42
random.seed(SEMILLA)
Faker.seed(SEMILLA)
fake = Faker('es_ES')

NUM_ENVIOS = 15_000
NUM_REPARTIDORES = 250

# Distribución de envíos por ciudad (refleja el peso real de cada mercado)
CIUDADES_PESOS = {
    'Madrid':    0.40,
    'Barcelona': 0.25,
    'Valencia':  0.15,
    'Sevilla':   0.12,
    'Bilbao':    0.08,
}

# Rangos de códigos postales por ciudad (aproximados, para que sean realistas)
CODIGOS_POSTALES = {
    'Madrid':    (28001, 28055),
    'Barcelona': (8001,  8042),
    'Valencia':  (46001, 46025),
    'Sevilla':   (41001, 41020),
    'Bilbao':    (48001, 48015),
}

# ------------------------------------------------------------------------------
# 1) GENERACIÓN DE REPARTIDORES (repartidores.json)
# ------------------------------------------------------------------------------
print("Generando repartidores...")

repartidores = []
ciudades_lista = list(CIUDADES_PESOS.keys())

for i in range(1, NUM_REPARTIDORES + 1):
    repartidor_id = f"REP-{i:03d}"
    ciudad = random.choices(ciudades_lista, weights=list(CIUDADES_PESOS.values()))[0]
    tipo_contrato = random.choices(
        ['EMPLEADO', 'AUTONOMO'], weights=[0.6, 0.4]
    )[0]
    vehiculo = random.choices(
        ['FURGONETA', 'MOTO', 'BICI'], weights=[0.5, 0.35, 0.15]
    )[0]
    fecha_alta = fake.date_between(start_date='-4y', end_date='-1m')

    repartidor = {
        'repartidor_id':   repartidor_id,
        'nombre':          fake.first_name(),
        'apellido':        fake.last_name() + " " + fake.last_name(),
        'fecha_alta':      fecha_alta.strftime('%Y-%m-%d'),
        'tipo_contrato':   tipo_contrato,
        'ciudad_asignada': ciudad,
        'vehiculo':        vehiculo,
        'telefono':        f"+34 {fake.msisdn()[3:12]}"
    }
    repartidores.append(repartidor)

# ----- PROBLEMAS DE CALIDAD EN REPARTIDORES -----

# Problema #10: nulos en teléfono (~10%)
# Decisión esperada en Silver: mantener nulo (no crítico para los KPIs).
for r in random.sample(repartidores, k=int(NUM_REPARTIDORES * 0.10)):
    r['telefono'] = None

# Problema #11: 3 registros duplicados
# Decisión esperada en Silver: dropDuplicates() sobre repartidor_id.
duplicados = random.sample(repartidores, 3)
repartidores.extend(duplicados)

# Problema #12: inconsistencias en tipo_contrato
# Decisión esperada en Silver: normalizar a mayúsculas y mapear "EMP" -> "EMPLEADO".
for r in random.sample(repartidores, 15):
    r['tipo_contrato'] = random.choice(['empleado', 'Empleado', 'EMP', 'autonomo'])

# Mezclamos para que los duplicados y problemas no estén al final
random.shuffle(repartidores)

# Escritura del JSON (formato JSON Lines: una línea por registro)
# Spark lee JSON Lines de forma nativa, es el formato estándar.
with open('repartidores.json', 'w', encoding='utf-8') as f:
    for r in repartidores:
        f.write(json.dumps(r, ensure_ascii=False) + '\n')

print(f"  -> repartidores.json generado: {len(repartidores)} registros "
      f"(250 únicos + 3 duplicados intencionales)")

# Lista de IDs válidos para usar en envios (sin duplicados)
ids_repartidores_validos = [f"REP-{i:03d}" for i in range(1, NUM_REPARTIDORES + 1)]

# ------------------------------------------------------------------------------
# 2) GENERACIÓN DE ENVÍOS (envios.csv)
# ------------------------------------------------------------------------------
print("Generando envíos...")

envios = []
fecha_inicio = datetime(2024, 1, 1)
fecha_fin    = datetime(2024, 12, 31)

# Distribución temporal con estacionalidad:
#   - picos en noviembre (Black Friday) y diciembre (Navidad)
#   - más envíos entre semana que fin de semana
def generar_fecha_con_estacionalidad():
    """Devuelve una fecha del año 2024 ponderada por estacionalidad realista."""
    while True:
        dia_aleatorio = fake.date_between(start_date=fecha_inicio, end_date=fecha_fin)
        # Peso por mes (Q4 más cargado)
        peso_mes = {
            1: 0.7, 2: 0.7, 3: 0.8, 4: 0.9, 5: 1.0, 6: 1.0,
            7: 0.9, 8: 0.7, 9: 1.0, 10: 1.2, 11: 1.6, 12: 1.5
        }[dia_aleatorio.month]
        # Peso por día de la semana (0=lunes, 6=domingo)
        peso_dia = [1.0, 1.0, 1.0, 1.0, 1.1, 0.6, 0.4][dia_aleatorio.weekday()]
        if random.random() < (peso_mes * peso_dia) / 2.0:
            return dia_aleatorio

for i in range(1, NUM_ENVIOS + 1):
    tracking_id = f"RPX-2024-{i:06d}"

    # Fecha de recogida con estacionalidad
    fecha_recogida_dia = generar_fecha_con_estacionalidad()
    hora_recogida = random.randint(8, 18)
    minuto = random.randint(0, 59)
    segundo = random.randint(0, 59)
    fecha_recogida = datetime.combine(
        fecha_recogida_dia,
        datetime.min.time()
    ).replace(hour=hora_recogida, minute=minuto, second=segundo)

    # Ciudad y código postal
    ciudad = random.choices(ciudades_lista, weights=list(CIUDADES_PESOS.values()))[0]
    cp_min, cp_max = CODIGOS_POSTALES[ciudad]
    codigo_postal = f"{random.randint(cp_min, cp_max):05d}"

    # Estado del envío (85% entregado, 10% incidencia, 5% devuelto)
    estado = random.choices(
        ['ENTREGADO', 'INCIDENCIA', 'DEVUELTO'],
        weights=[0.85, 0.10, 0.05]
    )[0]

    # Fecha de entrega: solo si está ENTREGADO; entre 4h y 36h después
    if estado == 'ENTREGADO':
        horas_transito = random.randint(4, 36)
        fecha_entrega = fecha_recogida + timedelta(hours=horas_transito,
                                                    minutes=random.randint(0, 59))
        fecha_entrega_str = fecha_entrega.strftime('%Y-%m-%d %H:%M:%S')
    else:
        # Problema #2 (nulos legítimos): los no entregados no tienen fecha_entrega
        fecha_entrega_str = None

    # Peso, importe, tipo de cliente
    peso_kg = round(random.uniform(0.2, 25.0), 2)
    cliente_tipo = random.choices(['PARTICULAR', 'EMPRESA'], weights=[0.7, 0.3])[0]
    importe_envio = round(random.uniform(3.5, 18.0), 2)
    repartidor_id = random.choice(ids_repartidores_validos)

    envio = {
        'tracking_id':    tracking_id,
        'fecha_recogida': fecha_recogida.strftime('%Y-%m-%d %H:%M:%S'),
        'fecha_entrega':  fecha_entrega_str,
        'ciudad':         ciudad,
        'codigo_postal':  codigo_postal,
        'peso_kg':        peso_kg,
        'estado':         estado,
        'repartidor_id':  repartidor_id,
        'cliente_tipo':   cliente_tipo,
        'importe_envio':  importe_envio,
    }
    envios.append(envio)

# ----- PROBLEMAS DE CALIDAD EN ENVÍOS -----

# Problema #1: nulos en peso_kg (~3%)
# Decisión esperada en Silver: imputar con la mediana del peso.
for e in random.sample(envios, k=int(NUM_ENVIOS * 0.03)):
    e['peso_kg'] = None

# Problema #3: duplicados exactos (~2%)
# Decisión esperada en Silver: dropDuplicates() sobre todas las columnas.
num_duplicados = int(NUM_ENVIOS * 0.02)
duplicados_envios = random.sample(envios, num_duplicados)
envios.extend([dict(e) for e in duplicados_envios])

# Problema #4: formato inconsistente de fechas en fecha_recogida (~10%)
# Decisión esperada en Silver: parsear con múltiples patrones (to_date con varios formatos).
for e in random.sample(envios, k=int(len(envios) * 0.10)):
    try:
        # Convertimos al formato europeo dd/MM/yyyy HH:mm:ss
        dt = datetime.strptime(e['fecha_recogida'], '%Y-%m-%d %H:%M:%S')
        e['fecha_recogida'] = dt.strftime('%d/%m/%Y %H:%M:%S')
    except (ValueError, TypeError):
        pass

# Problema #5: inconsistencia en mayúsculas/minúsculas/espacios en ciudad (~8%)
# Decisión esperada en Silver: trim + initcap o lower y mapear a forma canónica.
for e in random.sample(envios, k=int(len(envios) * 0.08)):
    transformacion = random.choice(['lower', 'upper', 'spaces'])
    if transformacion == 'lower':
        e['ciudad'] = e['ciudad'].lower()
    elif transformacion == 'upper':
        e['ciudad'] = e['ciudad'].upper()
    else:
        e['ciudad'] = f"  {e['ciudad']}  "

# Problema #6: códigos postales con 4 dígitos en lugar de 5 (~4%)
# Decisión esperada en Silver: lpad con ceros hasta 5 caracteres.
for e in random.sample(envios, k=int(len(envios) * 0.04)):
    if e['codigo_postal'].startswith('0'):
        e['codigo_postal'] = e['codigo_postal'].lstrip('0')

# Problema #7: pesos con coma decimal española en vez de punto (afecta a algunos)
# Decisión esperada en Silver: regexp_replace + cast a double.
# Nota: marcamos como string solo en algunos para forzar tipo inconsistente al leer CSV.
for e in random.sample([e for e in envios if e['peso_kg'] is not None], 200):
    e['peso_kg'] = str(e['peso_kg']).replace('.', ',')

# Problema #8: pesos fuera de rango (~0.5%) - negativos o absurdos
# Decisión esperada en Silver: filtrar con condición de rango razonable.
for e in random.sample(envios, k=int(len(envios) * 0.005)):
    e['peso_kg'] = random.choice([-5.0, -1.2, 9999.0, 5000.0])

# Problema #9: foreign key rota (~1%) - repartidor_id que no existe
# Decisión esperada en Silver: left join + filtro o marcado de registros huérfanos.
for e in random.sample(envios, k=int(len(envios) * 0.01)):
    e['repartidor_id'] = f"REP-{random.randint(900, 999):03d}"

# Mezclamos para que los problemas no estén concentrados
random.shuffle(envios)

# Escritura del CSV
# Usamos QUOTE_MINIMAL: solo entrecomilla cuando el valor contiene el separador.
# Esto simula un CSV "real" exportado de un sistema heredado.
with open('envios.csv', 'w', encoding='utf-8', newline='') as f:
    writer = csv.DictWriter(
        f,
        fieldnames=[
            'tracking_id', 'fecha_recogida', 'fecha_entrega', 'ciudad',
            'codigo_postal', 'peso_kg', 'estado', 'repartidor_id',
            'cliente_tipo', 'importe_envio'
        ],
        quoting=csv.QUOTE_MINIMAL
    )
    writer.writeheader()
    writer.writerows(envios)

print(f"  -> envios.csv generado: {len(envios)} filas "
      f"({NUM_ENVIOS} envíos únicos + {num_duplicados} duplicados intencionales)")

# ------------------------------------------------------------------------------
# 3) RESUMEN FINAL
# ------------------------------------------------------------------------------
print("\n" + "=" * 70)
print("GENERACIÓN COMPLETADA")
print("=" * 70)
print(f"  Ficheros generados:")
print(f"    - envios.csv         ({len(envios):>6} filas)")
print(f"    - repartidores.json  ({len(repartidores):>6} registros)")
print(f"\n  Problemas de calidad introducidos: 12 (ver comentarios en el código)")
print("=" * 70)
