# Value Props Ranking Pipeline

Pipeline de ingeniería de datos para el ranking de value propositions basado en comportamiento de usuarios.

## Estructura del Proyecto

```
value-prop-ranking-pipeline/
├── data/                   # Datos fuente (no modificar)
│   ├── prints.json
│   ├── taps.json
│   └── pays.csv
├── src/                    # Código fuente principal
│   ├── __init__.py
│   ├── config.py          # Configuración del proyecto
│   ├── io_utils.py        # Utilidades de I/O
│   ├── feature_engineering.py  # Ingeniería de features
│   ├── pipeline.py        # Pipeline principal (Pandas)
│   ├── spark_pipeline.py  # Pipeline alternativo (Spark)
│   └── utils.py           # Utilidades generales
├── scripts/               # Scripts de ejecución
│   ├── run_spark_pipeline.py
│   └── compare_pipelines.py
├── tests/                 # Tests unitarios
│   ├── test_pipeline.py
│   └── test_features.py
├── notebooks/             # Notebooks de exploración
│   ├── pipeline_pandas.py
│   └── pipeline_pyspark.py
├── output/                # Resultados generados
├── logs/                  # Logs de ejecución
├── models/                # Modelos entrenados (futuro)
├── main.py               # Script principal
├── requirements.txt      # Dependencias
└── README.md
```

## Instalación

1. **Clonar el repositorio:**
```bash
git clone <repository-url>
cd value-prop-ranking-pipeline
```

2. **Crear entorno virtual:**
```bash
python -m venv venv
source venv/bin/activate  # En Windows: venv\Scripts\activate
```

3. **Instalar dependencias:**
```bash
pip install -r requirements.txt
```

## Uso

### Pipeline Principal (Pandas)

```bash
python main.py
```

### Pipeline Spark (para datasets grandes)

```bash
python scripts/run_spark_pipeline.py
```

### Comparar resultados

```bash
python scripts/compare_pipelines.py
```

### Ejecutar tests

```bash
pytest tests/
```

## 🔧 Configuración

El archivo `src/config.py` contiene todas las configuraciones del proyecto:

- **Rutas de datos**: Ubicación de archivos fuente
- **Ventanas temporales**: Configuración de análisis histórico
- **Configuración Spark**: Parámetros para procesamiento distribuido
- **Columnas finales**: Esquema del dataset de salida

## Features Generadas

El pipeline genera las siguientes features:

| Feature | Descripción |
|---------|-------------|
| `user_id` | Identificador único del usuario |
| `value_prop_id` | Identificador de la value proposition |
| `timestamp` | Timestamp del evento |
| `clicked` | Flag binario (1 si hubo click, 0 si no) |
| `print_count_3w` | Número de prints en las últimas 3 semanas |
| `tap_count_3w` | Número de taps en las últimas 3 semanas |
| `pay_count_3w` | Número de pagos en las últimas 3 semanas |
| `total_amount_3w` | Monto total pagado en las últimas 3 semanas |

## Validaciones

El pipeline incluye múltiples validaciones:

- **Validación de esquema**: Verifica que los datos tengan la estructura esperada
- **Validación de calidad**: Detecta valores faltantes y anomalías
- **Validación de integridad**: Asegura consistencia entre datasets
- **Validación de rangos**: Verifica que las fechas y valores numéricos sean válidos

## Reportes

El pipeline genera automáticamente:

- **Dataset final**: CSV con todas las features
- **Reporte resumen**: Estadísticas descriptivas
- **Metadatos**: Información del procesamiento
- **Logs**: Trazabilidad completa del proceso

## Testing

```bash
# Ejecutar todos los tests
pytest

# Ejecutar tests específicos
pytest tests/test_pipeline.py

# Con cobertura
pytest --cov=src tests/
```

## Pipeline de Datos

### 1. Carga de Datos
- **Prints**: Eventos de visualización de value propositions
- **Taps**: Eventos de click en value propositions  
- **Pays**: Eventos de pago asociados a value propositions

### 2. Procesamiento
- **Limpieza**: Validación y corrección de datos
- **Feature Engineering**: Creación de features históricas
- **Agregación**: Cálculo de métricas por usuario y value prop

### 3. Validación
- **Calidad**: Verificación de integridad de datos
- **Consistencia**: Validación de rangos y tipos
- **Completitud**: Verificación de valores faltantes

### 4. Salida
- **Dataset final**: CSV listo para modelado
- **Reportes**: Documentación del procesamiento
- **Logs**: Trazabilidad del proceso

## Optimizaciones

### Pandas (Recomendado para datasets < 10GB)
- Procesamiento en memoria
- Optimizado para velocidad
- Fácil debugging y desarrollo

### Spark (Recomendado para datasets > 10GB)
- Procesamiento distribuido
- Escalabilidad horizontal
- Manejo de memoria eficiente

## Logs

Los logs se guardan en `logs/` y `output/` con diferentes niveles:

- **INFO**: Progreso del pipeline
- **WARNING**: Problemas no críticos
- **ERROR**: Errores que requieren atención
- **DEBUG**: Información detallada para debugging

## Contribución

1. Fork el proyecto
2. Crear una rama para tu feature (`git checkout -b feature/AmazingFeature`)
3. Commit tus cambios (`git commit -m 'Add some AmazingFeature'`)
4. Push a la rama (`git push origin feature/AmazingFeature`)
5. Abrir un Pull Request

## Licencia

Este proyecto está bajo la Licencia MIT - ver el archivo [LICENSE](LICENSE) para detalles.

## Soporte

Para reportar bugs o solicitar features, por favor crear un issue en el repositorio.

## Documentación Adicional

- [Guía de Configuración](docs/CONFIGURATION.md)
- [API Reference](docs/API.md)
- [Troubleshooting](docs/TROUBLESHOOTING.md)

---
