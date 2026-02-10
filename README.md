# unidad1-project

[LInk a los datos](https://www.kaggle.com/datasets/shivamb/netflix-shows)


- [ ] Estructura de paquete de python
- [ ] Lectura de CSV
- [ ] Separar el CSV en dos diferentes
- [ ] ¿? Extensión a ficheros excel

## Roadmap por Fases

### Fase 1: Fundamentos
**Objetivo:** Estructura del proyecto funcionando

**Tareas:**
- Crear estructura de carpetas con src layout
- Configurar pyproject.toml con dependencias básicas
- Crear módulos vacíos con __init__.py
- Configurar herramientas de calidad (ruff, pyright)
- Verificar que los imports funcionan entre módulos

**Checkpoint:** Puedes importar módulos entre sí sin errores

---

### Fase 2: Código Pythónico
**Objetivo:** Implementar lectura eficiente de datos

**Tareas:**
- Implementar un reader básico para CSV que use generadores
- El reader debe hacer yield de cada fila, no cargar todo en memoria
- Crear un context manager para abrir/cerrar archivos automáticamente
- Usar comprehensions para filtrado básico
- Añadir un decorador simple para logging de operaciones

**Tips:**
- Piensa en archivos grandes: ¿cómo procesarías un CSV de 10GB?
- Los generadores son tu mejor amigo para streaming
- El context manager debe garantizar que los archivos se cierran

**Checkpoint:** Puedes leer un CSV grande línea por línea sin problemas de memoria

---

### Fase 3: Código Limpio
**Objetivo:** Código legible y robusto

**Tareas:**
- Refactorizar funciones grandes en funciones pequeñas y específicas
- Cada función debe hacer UNA cosa bien
- Renombrar variables con nombres descriptivos (nada de `data`, `tmp`, `x`)
- Crear excepciones custom para errores específicos
- Añadir docstrings en formato Sphinx a todas las funciones públicas
- Implementar validación de inputs con error handling

**Tips:**
- Si una función tiene más de 20 líneas, probablemente hace demasiado
- Los nombres deben explicar QUÉ es, no CÓMO se usa
- Las excepciones custom ayudan a debuggear: `InvalidSchemaError` vs `ValueError`

**Checkpoint:** Otro desarrollador puede leer tu código y entenderlo sin preguntar

---

### Fase 4: Diseño
**Objetivo:** Arquitectura extensible con OOP

**Tareas:**
- Crear clases base abstractas (ABC) para Reader, Transformer, Writer
- Implementar clases concretas que hereden de las ABCs
- Usar Pydantic para modelos de datos y validación automática
- Aplicar composición: Pipeline compone múltiples Transformers
- Cada clase debe tener una sola responsabilidad (SRP)

**Tips:**
- Las ABCs definen el contrato: qué métodos DEBEN implementar las subclases
- Pydantic valida automáticamente: aprovéchalo para schemas de datos
- Composición > Herencia: un Pipeline "tiene" Transformers, no "es" un Transformer
- Si una clase hace muchas cosas, divídela

**Checkpoint:** Puedes añadir un nuevo Reader sin modificar código existente

---

### Fase 5: Testing y Optimización
**Objetivo:** Código confiable y eficiente

**Tareas:**
- Escribir tests para cada componente (readers, transformers, writers)
- Crear fixtures con datos de prueba
- Usar mocking para filesystem (no crear archivos reales en tests)
- Alcanzar 80%+ de cobertura de código
- Optimizar transformaciones con pandas cuando sea apropiado

**Tips:**
- Un test debe probar UNA cosa
- Los fixtures evitan duplicar código de setup
- Mock filesystem: usa pytest-mock o unittest.mock
- pandas es excelente para agregaciones, pero no siempre necesario

**Checkpoint:** Todos los tests pasan y tienes 80%+ de cobertura

---

### Fase 6: Integración
**Objetivo:** Paquete production-ready

**Tareas:**
- Implementar CLI con argparse o typer
- El CLI debe permitir ejecutar pipelines desde línea de comandos
- Escribir README completo con ejemplos de uso
- Crear ejemplos funcionales en carpeta examples/
- Preparar el paquete para distribución (build)

**Tips:**
- El CLI es la cara de tu paquete: hazlo intuitivo
- El README debe tener: instalación, quick start, ejemplos, API reference
- Los ejemplos deben ser copy-paste y funcionar
- Prueba instalar tu paquete en un entorno limpio

**Checkpoint:** Alguien puede instalar tu paquete y usarlo sin ayuda

---

## Funcionalidades Mínimas Requeridas

### Readers
- [ ] Leer CSV con generadores (yield por fila)
- [ ] Leer JSON (puede cargar todo, archivos pequeños)
- [ ] Manejo de errores de archivo no encontrado
- [ ] Detección automática de delimitadores (CSV)

### Transformers
- [ ] Filtrar filas por condición
- [ ] Seleccionar columnas específicas
- [ ] Transformar valores (ej: normalizar strings)
- [ ] Agregaciones básicas (suma, promedio por grupo)

### Writers
- [ ] Escribir CSV
- [ ] Escribir JSON
- [ ] Crear archivo si no existe
- [ ] Manejo de errores de escritura

### Pipeline
- [ ] Componer Reader + Transformers + Writer
- [ ] Ejecutar pipeline completo
- [ ] Logging de operaciones
- [ ] Manejo de errores en cualquier etapa

### Validación
- [ ] Validar schema de datos con Pydantic
- [ ] Validar tipos de columnas
- [ ] Reportar errores de validación claramente
