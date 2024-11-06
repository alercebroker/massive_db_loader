# Massive DB Loader

Esta es una herramienta que permite cargar un gran volumen de datos a partir de
parquets a tablas en una base de datos PostgreSQL.
Esta herramienta cuenta con una interfaz mediante CLI, al igual que una libreria
para uso programatico.

Las tablas a las que se le cargaran datos deben existir anteriormente en la base
de datos.

## Instalacion

Para instalar la herramienta basta con ejecutar:

```bash
poetry install
```

## Ejecucion

El programa puede ser ejecutado con el siguiente commando:

```bash
poetry run mdbl data-load --parquets <parquets_folder> --mapping <mappings.toml>
```

### parquets

La flag `--parquets` recibe la direccion a la carpeta base en donde se encuentran
los parquets, estos deben a su vez encontrarse en sub carpetas (el nombre de estas
es el que se utiliza en los mappings).

Un ejemplo de `parquets_folder` puede ser el siguiente:

- `/parquets`
  - `/parquet1`
    - `01.parquet`
    - `02.parquet`
    - `03.parquet`
  - `/parquet2`
    - `a.parquet`
    - `b.parquet`
    - `c.parquet`


el nombre de los archivos dentre de las sub carpetas no es importante.

Esta flag es opcional y por defecto se busca en el directorio `./parquets`.

### mapping

La flag `--mapping` recibe un archivo de mappings el cual puede ser un archivo 
`TOML` o `YAML` que indica los mappings. Este campo es obligatorio.

Ejemplo de formato de archivo de mappings:

```TOML
# TOML
[[tables]]
from = "parquet1"
to = "table1"
[[tables.columns]]
from = "column_in_parquet_1"
to = "column_in_table_1"
[[tables.columns]]
from = "column_in_parquet_2"
to = "column_in_table_2"

[[tables]]
from = "parquet2"
to = "table2"
[[tables.columns]]
from = "column_in_parquet_1"
to = "column_in_table1"
```

```YAML
# YAML
tables:
    - from: parquet1
      to: table1
      columns:
        - from: column_in_parquet_1
          to: column_in_table_1
        - from: column_in_parquet_2
          to: column_in_table_2
    - from: parquet2
      to: table2
      columns:
        - from: column_in_parquet_1
          to: column_in_table_1
```

> [!WARNING]
> El orden en el que se listan las tablas corresponde al orden en que se ejecutaran
> las inserciones. Esto se debe tener en consideracion a la hora de tratar con llaves
> primarias y foraneas.

### Configuracion DB

Para conectarse a la base de datos el programa lee las siguientes variables de entorno
como configuracion:

```bash
DB_NAME="postgres"
DB_USER="postgres"
DB_HOST="localhost"
DB_PORT="5432"
DB_PASS="postgres"
```

Ademas es posible configurar estas variables directamente al llamar la aplicacion
de la siguiente forma.

```bash
poetry run mdbl \
  --db-name postgres \
  --db-user postgres \
  --db-host localhost \
  --db-port 5432 \
  --db-pass postgres \
  data-load --mapping mapping.yaml
```
