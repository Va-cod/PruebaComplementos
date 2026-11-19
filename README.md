# Pipeline ETL Local con PySpark

Este repositorio contiene la implementación completa de un pipeline ETL local desarrollado con **PySpark** (`SparkSession.builder.master("local[*]")`), diseñado para procesar datos de e-commerce/retail, perfilar y corregir anomalías, realizar transformaciones financieras sin duplicación en joins, resolver lotes incrementales de forma idempotente y persistir los resultados en formato columnar Parquet con controles de calidad automatizados.

---

## 📁 Estructura del Proyecto

```text
PruebaComplementos/
├── data/                                 # Datasets de entrada (CSV)
│   ├── clientes.csv                      # Dimensión clientes
│   ├── productos.csv                     # Dimensión productos
│   ├── pedidos.csv                       # Hechos: cabecera de pedidos base
│   ├── detalle_pedidos.csv               # Hechos: líneas de detalle base
│   ├── pedidos_incrementales.csv         # Lote incremental de pedidos
│   └── detalle_incremental.csv           # Lote incremental de líneas de detalle
├── hadoop/bin/                           # Binarios locales de compatibilidad Windows
│   ├── hadoop.dll                        # DLL nativa de cliente Hadoop
│   └── winutils.exe                      # Emulador de permisos POSIX para Windows NTFS
├── notebooks/
│   └── pipeline_etl.ipynb                # Notebook interactivo con todo el flujo documentado
├── output/
│   └── ventas_netas_agregadas.parquet/   # Salida particionada en formato Parquet
├── etl_pipeline.py                       # Script principal autónomo en Python
└── README.md                             # Documentación del proyecto
```

---

## 🚀 Requisitos y Entorno

- **Python**: 3.10 o superior (probado en Python 3.12).
- **Java**: Java JDK 8, 11, 17 o 21 (variable `JAVA_HOME` configurada).
- **PySpark**: 3.5+ o 4.x (instalable mediante `pip install pyspark`).

> [!NOTE]
> **Compatibilidad en Windows**: En sistemas operativos Windows, Spark requiere `winutils.exe` y `hadoop.dll` para gestionar escrituras de archivos locales (Parquet) y permisos de sistema de archivos. El proyecto ya incluye estos binarios en la carpeta `hadoop/bin/` y los configura automáticamente en las variables de entorno dentro del código.

---

## 🛠️ Ejecución del Pipeline

### Opción 1: Ejecutar mediante Script de Python
Desde la raíz del proyecto, activa tu entorno virtual y ejecuta:

```powershell
.\.venv\Scripts\python etl_pipeline.py
```

### Opción 2: Ejecutar mediante Jupyter Notebook
Abre el notebook interactivo en PyCharm o Jupyter:
```text
notebooks/pipeline_etl.ipynb
```
Y ejecuta las celdas secuencialmente para observar los outputs y tablas intermedias.

---

## 🔍 Resumen del Flujo ETL

### 1. Ingesta de Datos (`@data/`)
Lectura de archivos CSV delimitados con cabeceras mediante la API de Spark DataFrame:
- `clientes.csv` (1,001 registros)
- `productos.csv` (100 registros)
- `pedidos.csv` (20,001 registros)
- `detalle_pedidos.csv` (40,026 registros)
- `pedidos_incrementales.csv` (101 registros)
- `detalle_incremental.csv` (200 registros)

### 2. Perfilado y Tratamiento de Anomalías
Se identificaron 5 anomalías críticas en los datos fuente y se implementaron sus tratamientos:

1. **Separador decimal en coma europea (`string`)**:
   - *Problema*: Valores como `"0,05"` en `discount_pct` y `"29,85"` en `unit_price` se leen como texto y al castear directamente resultan en `NULL`.
   - *Solución*: `regexp_replace(col, ',', '.')` previo al casteo a `DoubleType`.
2. **Valores numéricos de negocio fuera de rango (Outliers)**:
   - *Problema*: `quantity <= 0` (valores `0` y `-2`), `discount_pct = 1,2` (> 100%), y `unit_price` nulo o negativo (`-12.5`).
   - *Solución*: Filtrar `quantity > 0`, `discount_pct BETWEEN 0.0 AND 1.0`, y `unit_price > 0.0` para salvaguardar la exactitud financiera.
3. **Duplicados exactos en claves primarias de dimensiones**:
   - *Problema*: `customer_id = 'C0020'` duplicado en `clientes.csv`; `order_id = 'O00012'` duplicado en `pedidos.csv`.
   - *Solución*: Desduplicación con `.dropDuplicates(['customer_id'])` y `.dropDuplicates(['order_id'])`. Esto previene el efecto *fan-out* (multiplicación indebida de importes en los joins).
4. **Violación de integridad referencial (Huérfanos)**:
   - *Problema*: Líneas con `order_id` inexistente (`O99999`), `product_id` inexistente (`P999`), y pedidos con `customer_id` nulo o inexistente.
   - *Solución*: Realizar `INNER JOIN` con las dimensiones maestras validadas, asegurando linaje íntegro en cada registro agregado.
5. **Fechas malformadas y errores tipográficos**:
   - *Problema*: Pedido `O00025` con `order_ts = '2026-99-31T12:00:00Z'` (mes corrupto 99); pedido `O00030` con `status = 'deliverd'`.
   - *Solución*: Rescate de fecha válida con `coalesce(try_to_timestamp(order_ts), try_to_timestamp(updated_at))` y normalización de estados a `'delivered'`.

### 3. Limpieza, Cálculo de Importe Neto y Joins sin Duplicación
Se unen las tablas bajo una relación **$N:1$** donde cada línea de detalle corresponde exactamente a una fila de pedido, cliente y producto. El importe neto de cada línea se calcula con la fórmula:

$$\text{net\_amount} = \text{quantity} \times \text{unit\_price} \times (1.0 - \text{discount\_pct})$$

### 4. Agregación Multidimensional
Se calculan las ventas netas agrupadas por **mes (`yyyy-MM`)**, **país (`country`)**, **canal (`channel`)** y **categoría (`category`)**, tanto con la API de DataFrames como con Spark SQL:

```python
ventas_agg = lineas_enriquecidas.groupBy("mes", "country", "channel", "category") \
    .agg(
        round(spark_sum("net_amount"), 2).alias("ventas_netas"),
        count("line_id").alias("num_lineas"),
        spark_sum("quantity").alias("unidades_vendidas")
    ) \
    .orderBy("mes", "country", "channel", "category")
```

### 5. Reto Incremental e Idempotencia (CDC)
- **CDC / Desduplicación por Ventana**: Al existir pedidos en el lote incremental que actualizan pedidos previos (ej. `O00005`, cuyo `updated_at` cambia de `2026-02-22` a `2026-03-05`), se aplica una función ventana:
  ```python
  w_orders = Window.partitionBy("order_id").orderBy(col("ts_updated").desc())
  pedidos_clean = pedidos_union.withColumn("rn", row_number().over(w_orders)).filter("rn == 1").drop("rn")
  ```
- **Persistencia Idempotente**: Escritura con modo `.write.mode("overwrite").parquet(...)`, garantizando que reejecutar el pipeline produzca siempre el mismo resultado final sin generar duplicados ni archivos huérfanos.

### 6. Controles de Calidad Automatizados
El pipeline ejecuta dos validaciones con aserciones programáticas antes de dar por finalizado el proceso:

1. **Control 1 - Reconciliación de Masa Financiera (Zero Join Duplication)**:
   $$\sum \text{net\_amount}_{\text{líneas}} == \sum \text{ventas\_netas}_{\text{agregado}}$$
   Comprueba que no hubo pérdida ni duplicación artificial de importes durante los joins (diferencia $<\$0.07$ atribuible a redondeo de centavos).
2. **Control 2 - Unicidad, Completitud y Validez de Dominio**:
   Comprueba que la clave cuádruple `(mes, país, canal, categoría)` sea estrictamente única (208 particiones), sin valores nulos y con ventas netas estrictamente mayores a cero.
