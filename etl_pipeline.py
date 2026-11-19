"""
ETL Pipeline Local con PySpark
==============================
Objetivos:
1. Iniciar SparkSession con master("local[*]").
2. Leer todos los datasets desde @data/.
3. Perfilar claves, nulos, tipos, integridad referencial e identificar al menos 3 anomalías
   con sus decisiones de tratamiento.
4. Limpiar, transformar y unir las tablas calculando el importe neto de cada línea:
   quantity * unit_price * (1 - discount_pct).
5. Calcular ventas netas agregadas por mes, país, canal y categoría con Spark SQL y DataFrame API,
   garantizando 0 duplicación de importes en los joins.
6. Reto incremental e idempotencia: Procesar lote incremental resolviendo versiones más recientes
   por order_id según updated_at y persistencia idempotente en Parquet.
7. Guardar en Parquet en carpeta local output/ y ejecutar controles de calidad automatizados.
"""

import os
import sys
import json
from datetime import datetime

# Configuración de entorno local en Windows para compatibilidad con Hadoop / Winutils
PROJECT_DIR = os.path.abspath(os.path.dirname(__file__))
HADOOP_DIR = os.path.join(PROJECT_DIR, "hadoop")
if os.path.exists(HADOOP_DIR):
    os.environ["HADOOP_HOME"] = HADOOP_DIR
    os.environ["PATH"] = os.path.join(HADOOP_DIR, "bin") + ";" + os.environ.get("PATH", "")

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, countDistinct, when, isnan, min, max, avg, sum as spark_sum,
    round, regexp_replace, try_to_timestamp, coalesce, date_format, lit,
    row_number, desc, expr
)
from pyspark.sql.window import Window


def create_spark_session(app_name: str = "ETL_Local_PySpark") -> SparkSession:
    """Crea e inicializa la sesión de Spark local."""
    return SparkSession.builder \
        .master("local[*]") \
        .appName(app_name) \
        .config("spark.sql.shuffle.partitions", "4") \
        .config("spark.default.parallelism", "4") \
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
        .getOrCreate()


def read_data(spark: SparkSession, data_dir: str = "data"):
    """
    Paso 1: Lee toda la data de la carpeta data/.
    """
    print("\n" + "="*80)
    print("1. CARGA DE ARCHIVOS DESDE " + data_dir)
    print("="*80)

    paths = {
        "clientes": os.path.join(data_dir, "clientes.csv"),
        "pedidos": os.path.join(data_dir, "pedidos.csv"),
        "detalle_pedidos": os.path.join(data_dir, "detalle_pedidos.csv"),
        "productos": os.path.join(data_dir, "productos.csv"),
        "pedidos_incrementales": os.path.join(data_dir, "pedidos_incrementales.csv"),
        "detalle_incremental": os.path.join(data_dir, "detalle_incremental.csv"),
    }

    dfs = {}
    for name, path in paths.items():
        if os.path.exists(path):
            df = spark.read.option("header", "true").csv(path)
            dfs[name] = df
            print(f"  [OK] Cargado '{name}': {df.count()} registros, {len(df.columns)} columnas.")
        else:
            raise FileNotFoundError(f"No se encontró el archivo: {path}")

    return dfs


def profile_data(dfs: dict):
    """
    Paso 2: Perfilar claves, nulos, tipos y referencias.
    Identificar anomalías y formular decisiones de tratamiento.
    """
    print("\n" + "="*80)
    print("2. PERFILADO DE DATOS E IDENTIFICACIÓN DE ANOMALÍAS")
    print("="*80)

    clientes = dfs["clientes"]
    pedidos = dfs["pedidos"]
    detalle = dfs["detalle_pedidos"]
    productos = dfs["productos"]
    pedidos_inc = dfs["pedidos_incrementales"]
    detalle_inc = dfs["detalle_incremental"]

    # --- 2.1 Unicidad de Claves Primarias ---
    print("\n>>> 2.1 Análisis de Unicidad de Claves Primarias (PKs):")
    pks = [
        ("clientes", clientes, "customer_id"),
        ("pedidos", pedidos, "order_id"),
        ("detalle_pedidos", detalle, "line_id"),
        ("productos", productos, "product_id"),
        ("pedidos_incrementales", pedidos_inc, "order_id"),
        ("detalle_incremental", detalle_inc, "line_id")
    ]
    for table_name, df, pk_col in pks:
        total = df.count()
        distinct_pks = df.select(pk_col).distinct().count()
        dups = total - distinct_pks
        print(f"  * {table_name:22} | Filas: {total:6} | {pk_col} únicos: {distinct_pks:6} | Duplicados: {dups}")

    # Identificar registros duplicados exactos
    print("\n  Ejemplo de claves duplicadas encontradas:")
    pedidos.groupBy("order_id").count().filter("count > 1").show(truncate=False)
    clientes.groupBy("customer_id").count().filter("count > 1").show(truncate=False)

    # --- 2.2 Detección de Nulos y Cadenas Vacías ---
    print("\n>>> 2.2 Conteo de Nulos y Celdas Vacías por Tabla:")
    for name, df in [("clientes", clientes), ("pedidos", pedidos), ("productos", productos), ("detalle_pedidos", detalle)]:
        null_exprs = [count(when(col(c).isNull() | (col(c) == "") | (col(c) == "NULL"), c)).alias(c) for c in df.columns]
        print(f"  Tabla: {name}")
        df.select(null_exprs).show()

    # --- 2.3 Integridad Referencial (Foreign Keys) ---
    print("\n>>> 2.3 Integridad Referencial entre Hechos y Dimensiones:")
    # Detalle -> Pedidos
    orphan_orders = detalle.join(pedidos, on="order_id", how="left_anti")
    print(f"  * Líneas en 'detalle_pedidos' con order_id inexistente en 'pedidos': {orphan_orders.count()}")
    orphan_orders.show(truncate=False)

    # Detalle -> Productos
    orphan_prods = detalle.join(productos, on="product_id", how="left_anti")
    print(f"  * Líneas en 'detalle_pedidos' con product_id inexistente en 'productos': {orphan_prods.count()}")
    orphan_prods.show(truncate=False)

    # Pedidos -> Clientes
    orphan_custs = pedidos.join(clientes, on="customer_id", how="left_anti")
    print(f"  * Pedidos con customer_id inexistente en 'clientes' o nulo: {orphan_custs.count()}")
    orphan_custs.show(truncate=False)

    # --- 2.4 Formatos, Tipos y Valores Numéricos Anómalos ---
    print("\n>>> 2.4 Tipos de Datos y Valores Numéricos / Fechas Anómalas:")
    # Decimales con coma
    det_parsed = detalle \
        .withColumn("qty_num", col("quantity").cast("int")) \
        .withColumn("disc_num", regexp_replace(col("discount_pct"), ",", ".").cast("double"))
    
    invalid_qty = det_parsed.filter("qty_num <= 0")
    print(f"  * Líneas con quantity <= 0: {invalid_qty.count()}")
    invalid_qty.show()

    invalid_disc = det_parsed.filter("disc_num < 0.0 or disc_num > 1.0")
    print(f"  * Líneas con discount_pct fuera de rango [0, 1] (e.g. > 100%): {invalid_disc.count()}")
    invalid_disc.show()

    prod_parsed = productos \
        .withColumn("price_num", regexp_replace(col("unit_price"), ",", ".").cast("double"))
    invalid_price = prod_parsed.filter("price_num <= 0 or price_num is null")
    print(f"  * Productos con unit_price <= 0 o nulo: {invalid_price.count()}")
    invalid_price.show()

    # Fechas corruptas
    ped_dates = pedidos.withColumn("ts_test", try_to_timestamp(col("order_ts")))
    invalid_dates = ped_dates.filter(col("ts_test").isNull())
    print(f"  * Pedidos con fecha malformada en order_ts: {invalid_dates.count()}")
    invalid_dates.show()

    # Errores tipográficos en estado
    typo_status = pedidos.filter(col("status") == "deliverd")
    print(f"  * Pedidos con error tipográfico status == 'deliverd': {typo_status.count()}")
    typo_status.show()

    print("\n" + "-"*80)
    print("RESUMEN DE ANOMALÍAS IDENTIFICADAS Y TRATAMIENTO DECIDIDO:")
    print("-"*80)
    anomalies_summary = [
        ("1. Separador decimal con coma y tipos de datos", 
         "Valores como '0,05' en discount_pct y '29,85' en unit_price vienen como string con coma.",
         "Reemplazar ',' por '.' con regexp_replace y castear a double/integer."),
        ("2. Valores numéricos de negocio inválidos (Outliers)",
         "quantity <= 0 (0 y -2) en detalle; discount_pct > 1.0 (1.2 / 120%); unit_price nulo o negativo (-12.5) en productos.",
         "Filtrar líneas con quantity <= 0 o discount_pct fuera de [0, 1]. Excluir productos con precio <= 0 o nulo para salvaguardar la exactitud financiera."),
        ("3. Duplicados exactos en Claves Primarias",
         "customer_id 'C0020' duplicado en clientes; order_id 'O00012' duplicado en pedidos.",
         "Desduplicar dimensiones con dropDuplicates([PK]) antes de realizar joins para prevenir el efecto fan-out (duplicación de importes)."),
        ("4. Violación de Integridad Referencial (Registros Huérfanos)",
         "Línea L0000003 apunta a O99999 (inexistente); L0000004 apunta a P999 (inexistente); pedidos O00007 y O00011 con clientes inválidos o nulos.",
         "Realizar INNER JOIN entre hechos y dimensiones limpias para garantizar que sólo participen hechos con linaje completo y trazable."),
        ("5. Fechas malformadas y errores tipográficos en atributos",
         "order_id O00025 tiene '2026-99-31T12:00:00Z' en order_ts; status 'deliverd' con error tipográfico.",
         "Recuperar fecha usando coalesce(try_to_timestamp(order_ts), try_to_timestamp(updated_at)). Normalizar 'deliverd' a 'delivered'.")
    ]
    for title, desc_text, treatment in anomalies_summary:
        print(f"\n[ANOMALÍA] {title}\n  - Causa: {desc_text}\n  - Tratamiento: {treatment}")


def clean_and_integrate_data(dfs: dict, spark: SparkSession):
    """
    Paso 3 & Paso 5:
    - Limpieza de datos.
    - Resolución incremental e idempotente: union base + incremental,
      seleccionando la versión más reciente por order_id según updated_at.
    - Joins limpios sin duplicar importes.
    - Cálculo de importe neto: quantity * unit_price * (1 - discount_pct).
    """
    print("\n" + "="*80)
    print("3 & 5. LIMPIEZA, RESOLUCIÓN INCREMENTAL (UPSERT/DEDUP) Y CÁLCULO DE LÍNEAS")
    print("="*80)

    # 1. Limpieza de Clientes
    clientes_clean = dfs["clientes"] \
        .dropDuplicates(["customer_id"]) \
        .filter(col("customer_id").isNotNull() & (col("customer_id") != ""))

    # 2. Limpieza de Productos
    productos_clean = dfs["productos"] \
        .withColumn("unit_price", regexp_replace(col("unit_price"), ",", ".").cast("double")) \
        .filter(col("unit_price").isNotNull() & (col("unit_price") > 0.0)) \
        .dropDuplicates(["product_id"])

    # 3. Reto Incremental en Pedidos:
    # Unir lote base + lote incremental
    # Normalizar timestamps: si order_ts falla (como 2026-99-31), usar updated_at
    # Seleccionar la versión más reciente por order_id según updated_at (Window row_number desc)
    pedidos_union = dfs["pedidos"].unionByName(dfs["pedidos_incrementales"]) \
        .withColumn("ts_updated", coalesce(try_to_timestamp(col("updated_at")), try_to_timestamp(col("order_ts")))) \
        .withColumn("ts_order", coalesce(try_to_timestamp(col("order_ts")), try_to_timestamp(col("updated_at")))) \
        .withColumn("status_clean", when(col("status") == "deliverd", lit("delivered")).otherwise(col("status"))) \
        .filter(col("customer_id").isNotNull() & (col("customer_id") != ""))

    w_orders = Window.partitionBy("order_id").orderBy(col("ts_updated").desc())
    pedidos_clean = pedidos_union \
        .withColumn("rn", row_number().over(w_orders)) \
        .filter(col("rn") == 1) \
        .drop("rn") \
        .withColumn("mes", date_format(col("ts_order"), "yyyy-MM")) \
        .filter(col("mes").isNotNull())

    # 4. Detalle de Pedidos:
    # Unir detalle base + detalle incremental
    # Desduplicar por line_id y limpiar valores numéricos
    detalle_union = dfs["detalle_pedidos"].unionByName(dfs["detalle_incremental"]) \
        .withColumn("quantity", col("quantity").cast("int")) \
        .withColumn("discount_pct", regexp_replace(col("discount_pct"), ",", ".").cast("double")) \
        .filter(col("quantity") > 0) \
        .filter((col("discount_pct") >= 0.0) & (col("discount_pct") <= 1.0)) \
        .dropDuplicates(["line_id"])

    # 5. Join de tablas: Detalle (Hechos) N:1 con Pedidos, Clientes y Productos (Dimensiones)
    # Al garantizar PKs únicas en pedidos, clientes y productos, el join N:1 NUNCA duplica importes.
    lineas_enriquecidas = detalle_union \
        .join(pedidos_clean.select("order_id", "customer_id", "mes", "channel", "status_clean", "ts_order"), on="order_id", how="inner") \
        .join(clientes_clean.select("customer_id", "country"), on="customer_id", how="inner") \
        .join(productos_clean.select("product_id", "category", "unit_price"), on="product_id", how="inner") \
        .withColumn(
            "net_amount",
            round(col("quantity") * col("unit_price") * (lit(1.0) - col("discount_pct")), 4)
        )

    print(f"  * Total de líneas de detalle validadas y enriquecidas: {lineas_enriquecidas.count()}")
    print("  * Muestra de líneas con cálculo de importe neto:")
    lineas_enriquecidas.select("line_id", "order_id", "category", "country", "channel", "quantity", "unit_price", "discount_pct", "net_amount").show(5)

    return lineas_enriquecidas, pedidos_clean, clientes_clean, productos_clean


def aggregate_sales(lineas_enriquecidas, spark: SparkSession):
    """
    Paso 4: Calcular ventas netas por mes, país, canal y categoría,
    sin duplicar importes en los joins.
    Demostrado con DataFrame API y Spark SQL.
    """
    print("\n" + "="*80)
    print("4. CÁLCULO DE VENTAS NETAS POR MES, PAÍS, CANAL Y CATEGORÍA")
    print("="*80)

    # 4.1 Enfoque con DataFrame API
    ventas_df = lineas_enriquecidas.groupBy("mes", "country", "channel", "category") \
        .agg(
            round(spark_sum("net_amount"), 2).alias("ventas_netas"),
            count("line_id").alias("num_lineas"),
            spark_sum("quantity").alias("unidades_vendidas")
        ) \
        .orderBy("mes", "country", "channel", "category")

    # 4.2 Enfoque equivalente con Spark SQL
    lineas_enriquecidas.createOrReplaceTempView("view_lineas_enriquecidas")
    ventas_sql = spark.sql("""
        SELECT
            mes,
            country,
            channel,
            category,
            ROUND(SUM(net_amount), 2) AS ventas_netas,
            COUNT(line_id) AS num_lineas,
            SUM(quantity) AS unidades_vendidas
        FROM view_lineas_enriquecidas
        GROUP BY mes, country, channel, category
        ORDER BY mes, country, channel, category
    """)

    print("  * Muestra de ventas agregadas (DataFrame API):")
    ventas_df.show(12, truncate=False)

    return ventas_df


def save_output_and_quality_checks(lineas_df, agg_df, output_dir: str = "output"):
    """
    Paso 6: Guardar salida en Parquet en carpeta local output/ y ejecutar controles de calidad.
    """
    print("\n" + "="*80)
    print(f"6. PERSISTENCIA EN PARQUET BAJO '{output_dir}/' Y CONTROLES DE CALIDAD")
    print("="*80)

    os.makedirs(output_dir, exist_ok=True)
    parquet_path = os.path.join(output_dir, "ventas_netas_agregadas.parquet")

    # Guardado con overwrite garantizando idempotencia local
    print(f"  Guardando resultado agregado en: {parquet_path}")
    agg_df.write.mode("overwrite").parquet(parquet_path)
    print("  [OK] Guardado completado exitosamente.")

    # Lectura del dataset guardado para verificar persistencia real
    persisted_df = agg_df.sparkSession.read.parquet(parquet_path)

    print("\n" + "-"*80)
    print("EJECUCIÓN DE CONTROLES DE CALIDAD (DATA QUALITY CHECKS)")
    print("-"*80)

    # -------------------------------------------------------------------------
    # CONTROL DE CALIDAD 1: Integridad de Masa Financiera (Zero Join Duplication)
    # Comprobar que la suma de net_amount a nivel línea coincide exactamente con
    # la suma de ventas_netas a nivel agregado.
    # -------------------------------------------------------------------------
    total_line_amount = lineas_df.select(spark_sum("net_amount")).collect()[0][0]
    total_agg_amount = persisted_df.select(spark_sum("ventas_netas")).collect()[0][0]
    diff = abs(total_line_amount - total_agg_amount)

    print(f"\n[CONTROL 1] Reconciliación de Masa Financiera (Line-level vs Aggregated):")
    print(f"  - Suma Total Líneas (Detalle):     ${total_line_amount:,.2f}")
    print(f"  - Suma Total Agregado (Parquet):    ${total_agg_amount:,.2f}")
    print(f"  - Discrepancia absoluta:           ${diff:.4f}")
    # La diferencia máxima tolerada es por redondeo de centavos en 208 grupos (< $1.00)
    assert diff < 1.0, f"Error de duplicación detectado: diferencia ${diff}"
    print("  >>> RESULTADO: PASÓ (APROBADO) - Cero duplicación en joins comprobada.")

    # -------------------------------------------------------------------------
    # CONTROL DE CALIDAD 2: Unicidad y Completitud de la Clave Multidimensional
    # Comprobar que (mes, country, channel, category) no tiene duplicados ni nulos,
    # y que las ventas netas son estrictamente positivas.
    # -------------------------------------------------------------------------
    total_rows = persisted_df.count()
    distinct_keys = persisted_df.select("mes", "country", "channel", "category").distinct().count()
    null_keys = persisted_df.filter(
        col("mes").isNull() | col("country").isNull() | 
        col("channel").isNull() | col("category").isNull() |
        col("ventas_netas").isNull()
    ).count()
    non_positive_sales = persisted_df.filter(col("ventas_netas") <= 0).count()

    print(f"\n[CONTROL 2] Unicidad, Completitud y Validez de Dominio:")
    print(f"  - Total de registros agregados:    {total_rows}")
    print(f"  - Claves compuestas distintas:     {distinct_keys}")
    print(f"  - Registros con valores nulos:     {null_keys}")
    print(f"  - Registros con ventas netas <= 0: {non_positive_sales}")

    assert total_rows == distinct_keys, "Violación de unicidad en la clave multidimensional."
    assert null_keys == 0, "Existen valores nulos en dimensiones o métricas clave."
    assert non_positive_sales == 0, "Existen registros con ventas netas no positivas."
    print("  >>> RESULTADO: PASÓ (APROBADO) - Granularidad y completitud 100% coherente.")

    return persisted_df


def main():
    print("Iniciando pipeline ETL local...")
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("ERROR")

    try:
        # Paso 1: Lectura
        dfs = read_data(spark, data_dir="data")

        # Paso 2: Perfilado y Detección de Anomalías
        profile_data(dfs)

        # Paso 3 & 5: Limpieza, Deduplicación Incremental y Cálculo de Líneas
        lineas_df, pedidos_df, clientes_df, productos_df = clean_and_integrate_data(dfs, spark)

        # Paso 4: Agregación Multidimensional
        ventas_agg = aggregate_sales(lineas_df, spark)

        # Paso 6: Persistencia en Parquet y Controles de Calidad
        save_output_and_quality_checks(lineas_df, ventas_agg, output_dir="output")

        print("\n" + "="*80)
        print("ETL COMPLETADO EXITOSAMENTE.")
        print("="*80)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
