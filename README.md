# Value Props Ranking Pipeline

Data engineering pipeline for value proposition ranking based on user behavior.

## Project Structure

```
value_props_pipeline/
├── data/                   # Source data (Not uploaded due to security issues)
│   ├── prints.json
│   ├── taps.json
│   └── pays.csv
├── src/                    # Main source code
│   ├── __init__.py
│   ├── config.py           # Project configuration
│   ├── io_utils.py         # I/O utilities
│   ├── feature_engineering.py  # Feature engineering
│   ├── pipeline.py         # Main pipeline (Pandas)
│   ├── spark_pipeline.py   # Alternative pipeline (Spark)
│   └── utils.py            # General utilities
├── scripts/                # Execution scripts
│   ├── run_spark_pipeline.py
│   ├── compare_pipelines.py
│   └── cleanup.py
├── tests/                  # Unit tests
│   └── test_pipeline.py
├── original_pipelines/     # Original pipeline implementations (reference)
├── output/                 # Generated results (datasets, reports)
├── logs/                   # Execution logs (pipeline.log, spark_pipeline.log, comparison.log)
├── models/                 # Trained models (future)
├── main.py                 # Main script
├── requirements.txt        # Dependencies
├── pyproject.toml          # Development tools configuration
├── Makefile                # Command automation
└── README.md
```

## Installation

1. **Clone the repository:**
```bash
git clone <repository-url>
cd value_props_pipeline
```

2. **Create virtual environment:**
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. **Install dependencies:**
```bash
pip install -r requirements.txt
```

## Usage

### Main Pipeline (Pandas)

```bash
python main.py
```

### Spark Pipeline (for large datasets)

```bash
python scripts/run_spark_pipeline.py
```

### Compare results

```bash
python scripts/compare_pipelines.py
```

### Run tests

```bash
pytest tests/
```

## 🔧 Configuration

The `src/config.py` file contains all project configurations:

- **Data paths**: Source file locations
- **Time windows**: Historical analysis configuration
- **Spark configuration**: Distributed processing parameters
- **Final columns**: Output dataset schema

## Generated Features

The pipeline generates the following features:

| Feature | Description |
|---------|-------------|
| `user_id` | Unique user identifier |
| `value_prop_id` | Value proposition identifier |
| `timestamp` | Event timestamp |
| `clicked` | Binary flag (1 if clicked, 0 if not) |
| `print_count_3w` | Number of prints in the last 3 weeks |
| `tap_count_3w` | Number of taps in the last 3 weeks |
| `pay_count_3w` | Number of payments in the last 3 weeks |
| `total_amount_3w` | Total amount paid in the last 3 weeks |

## Validations

The pipeline includes multiple validations:

- **Schema validation**: Verifies data has expected structure
- **Quality validation**: Detects missing values and anomalies
- **Integrity validation**: Ensures consistency between datasets
- **Range validation**: Verifies dates and numeric values are valid

## Reports

The pipeline automatically generates:

- **Final dataset**: CSV with all features (in `output/`)
- **Summary report**: Descriptive statistics
- **Metadata**: Processing information
- **Logs**: Complete process traceability (in `logs/`)

## Testing

```bash
# Run all tests
pytest

# Run specific tests
pytest tests/test_pipeline.py

# With coverage
pytest --cov=src tests/
```

## Data Pipeline

### 1. Data Loading
- **Prints**: Value proposition view events
- **Taps**: Value proposition click events  
- **Pays**: Payment events associated with value propositions

### 2. Processing
- **Cleaning**: Data validation and correction
- **Feature Engineering**: Historical feature creation
- **Aggregation**: Metrics calculation by user and value prop

### 3. Validation
- **Quality**: Data integrity verification
- **Consistency**: Range and type validation
- **Completeness**: Missing value verification

### 4. Output
- **Final dataset**: CSV ready for modeling (in `output/`)
- **Reports**: Processing documentation
- **Logs**: Process traceability (in `logs/`)

## Optimizations

### Pandas (Recommended for datasets < 10GB)
- In-memory processing
- Optimized for speed
- Easy debugging and development

### Spark (Recommended for datasets > 10GB)
- Distributed processing
- Horizontal scalability
- Efficient memory management

## Logs

Logs are saved in the `logs/` folder with different levels:

- **INFO**: Pipeline progress
- **WARNING**: Non-critical issues
- **ERROR**: Issues requiring attention
- **DEBUG**: Detailed debugging information

## Contributing

1. Fork the project
2. Create a feature branch (`git checkout -b feature/AmazingFeature`)
3. Commit your changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

## License

This project is under the MIT License - see the [LICENSE](LICENSE) file for details.

## Support

To report bugs or request features, please create an issue in the repository.

---
