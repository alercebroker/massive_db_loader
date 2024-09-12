# massive_db_loader

Esta es una herramienta que permite cargar un gran volumen de parquets en una base de datos PostgreSQL **nueva? existente?**.
Durante el proceso de carga se pueden definir mappings de columnas del schema del parquet al schema de la base de datos objetivo mediante un archivo de config.

Esta herramienta cuenta con una interfaz mediante CLI, al igual que una libreria para uso programatico.

## Mappings

> [!WARNING]
> Esta seccion se encuentra WIP

Se planea soportar mappings en TOML o YAML.
```TOML
# TOML
[nombre_tabla]
stage = 0
[tabla.mappings]
columna1 = { column = "columna_parquet3", type="int" }
columna2 = { column = "columna_parquet4", type="int" }
```
```YAML
# YAML
nombre_tabla:
    stage: 0
    mappings:
        columna1:
            column: "columna_parquet3"
            type: "int"
        columna2:
            column: "columna_parquet4"
            type: "int"
```
(stage en este caso seria el orden en que se deben ejecutar las cargas)

## Detalles de implementacion

Esta herramienta estara escrita en Python, utilizando la libreria Click para ofrecer su CLI.
Utilizaremos duckdb para ejecutar SQL en las bases de datos directamente, sin utilizar un ORM de por medio.
