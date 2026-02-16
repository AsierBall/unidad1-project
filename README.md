# Unidad 1 Project: Netflix ETL

Este proyecto implementa una tubería (pipeline) ETL (Extract, Transform, Load) para procesar datos de títulos de Netflix, utilizando un enfoque modular y orientado a objetos.

## Descripción

El objetivo es proporcionar una herramienta extensible para la limpieza y transformación de datos. El sistema está diseñado en componentes desacoplados:

- **Readers**: Lectura de fuentes de datos (CSV, JSON) de manera eficiente (por chunks).
- **Transformers**: Aplicación de reglas de negocio y limpieza de datos.
- **Writers**: Exportación de los datos procesados (CSV, JSON).
- **Orchestrator**: Coordinación del flujo de trabajo completo.

## Estructura del Proyecto

```
unidad1-project/
├── data/               # Archivos de entrada y salida (ej. netflix_titles.csv)
├── src/
│   └── unidad1_project/
│       ├── orchestrator/   # Orquestador del pipeline
│       ├── readers/        # Lectores de datos (CSVReaderPandas, etc.)
│       ├── transformers/   # Transformadores de datos
│       ├── writers/        # Escritores de datos
│       ├── logging/        # Configuración de logs
│       └── __init__.py     # Exportación de componentes principales
├── tests/              # Tests
├── main.py             # Script de ejemplo de uso
├── pyproject.toml      # Gestión de dependencias y configuración
└── README.md           # Documentación del proyecto
```

## Requisitos

- `uv` (Gestor de proyectos Python recomendado)
- Opcionalmente: Python >= 3.11 si no usas `uv`

## Instalación

Este proyecto utiliza `uv` para la gestión de dependencias y entornos virtuales.

1.  **Clonar el repositorio**:
    ```bash
    git clone <url-del-repo>
    cd unidad1-project
    ```

2.  **Instalar dependencias**:
    ```bash
    uv sync
    ```
    
    > **Nota**: Si no usas `uv`, puedes instalar las dependencias manualmente desde `pyproject.toml` con `pip install .` o `pip install pandas numpy`.

## Uso

### Ejecución Básica

El archivo `main.py` contiene un ejemplo completo de uso que:
1.  Lee el archivo `data/netflix_titles.csv`.
2.  Aplica transformaciones (completar valores nulos, normalizar cadenas).
3.  Guarda el resultado en `data/netflix_titles_wr.csv`.

Para ejecutarlo:

```bash
python main.py
```

### Uso como Librería

Puedes importar los componentes en tus propios scripts para construir pipelines personalizados:

```python
from pathlib import Path
from unidad1_project import (
    CSVReaderPandas,
    TransformerNormalizeStrings,
    WriterCsv,
    Orchestrator
)

# 1. Definir rutas
input_path = Path("mis_datos.csv")
output_path = Path("datos_limpios.csv")

# 2. Configurar componentes
# Lee el archivo en trozos de 1000 filas
reader = CSVReaderPandas(chunk_size=1000)

# Lista de transformaciones a aplicar secuencialmente
transformers = [
    TransformerNormalizeStrings()
    # Agrega más transformadores aquí
]

writer = WriterCsv()

# 3. Crear y ejecutar el orquestador
orchestrator = Orchestrator(
    reader=reader,
    transformers=transformers,
    writer=writer
)

orchestrator.run(input_path, output_path)
```

## Desarrollo

Para ejecutar los tests y asegurar la calidad del código:

```bash
# Ejecutar tests
pytest

# Verificar tipos y estilo
ruff check .
pyright

# Ejecutar tests con cobertura
pytest --cov=src --cov-report=term-missing -v
```

## Autores

- Ander Góngora Allué
- Raquel Roy Rubio
- Asier Ballesteros Domínguez
