"""
Tests para el pipeline de Value Props Ranking.
"""

import pytest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from unittest.mock import patch, MagicMock
import sys
from pathlib import Path

# Añadir src al path
sys.path.append(str(Path(__file__).parent.parent / "src"))

from src.pipeline import build_dataset, run_pipeline_with_validation
from src.feature_engineering import add_click_flag, add_historical_features, create_features_pipeline
from src.io_utils import load_data, validate_data
from src.utils import calculate_data_quality_metrics, validate_dataset_schema

class TestPipeline:
    """Tests para el pipeline principal."""
    
    @pytest.fixture
    def sample_data(self):
        """Crear datos de muestra para testing."""
        # Crear datos de prints
        prints_data = {
            'user_id': ['user1', 'user2', 'user1', 'user3'],
            'value_prop_id': ['prop1', 'prop2', 'prop1', 'prop3'],
            'timestamp': [
                datetime.now() - timedelta(days=1),
                datetime.now() - timedelta(days=2),
                datetime.now() - timedelta(days=3),
                datetime.now() - timedelta(days=4)
            ],
            'event_data': [
                {'value_prop': 'prop1'},
                {'value_prop': 'prop2'},
                {'value_prop': 'prop1'},
                {'value_prop': 'prop3'}
            ]
        }
        
        # Crear datos de taps
        taps_data = {
            'user_id': ['user1', 'user2'],
            'value_prop_id': ['prop1', 'prop2'],
            'timestamp': [
                datetime.now() - timedelta(days=1),
                datetime.now() - timedelta(days=2)
            ],
            'event_data': [
                {'value_prop': 'prop1'},
                {'value_prop': 'prop2'}
            ]
        }
        
        # Crear datos de pays
        pays_data = {
            'user_id': ['user1', 'user2'],
            'value_prop_id': ['prop1', 'prop2'],
            'timestamp': [
                datetime.now() - timedelta(days=5),
                datetime.now() - timedelta(days=6)
            ],
            'amount': [100.0, 200.0]
        }
        
        return (
            pd.DataFrame(prints_data),
            pd.DataFrame(taps_data),
            pd.DataFrame(pays_data)
        )
    
    def test_add_click_flag(self, sample_data):
        """Test para añadir flag de click."""
        prints, taps, _ = sample_data
        
        result = add_click_flag(prints, taps)
        
        assert 'clicked' in result.columns
        assert result['clicked'].dtype == int
        assert result['clicked'].sum() == 2  # 2 clicks esperados
        assert len(result) == len(prints)
    
    def test_add_historical_features(self, sample_data):
        """Test para añadir features históricas."""
        prints, taps, pays = sample_data
        
        # Definir ventanas temporales
        end_date = prints['timestamp'].max()
        start_last_week = end_date - timedelta(days=7)
        start_3weeks_ago = end_date - timedelta(days=21)
        
        # Filtrar prints recientes
        recent_prints = prints[prints['timestamp'] >= start_last_week].copy()
        
        # Añadir features de prints
        result = add_historical_features(
            recent_prints, prints, start_3weeks_ago, start_last_week,
            {'print_count_3w': {'agg_func': 'count'}}
        )
        
        assert 'print_count_3w' in result.columns
        assert result['print_count_3w'].dtype in [int, float]
        assert (result['print_count_3w'] >= 0).all()
    
    def test_create_features_pipeline(self, sample_data):
        """Test para el pipeline completo de features."""
        prints, taps, pays = sample_data
        
        result = create_features_pipeline(prints, taps, pays)
        
        expected_columns = [
            'user_id', 'value_prop_id', 'timestamp', 'clicked',
            'print_count_3w', 'tap_count_3w', 'pay_count_3w', 'total_amount_3w'
        ]
        
        for col in expected_columns:
            assert col in result.columns
        
        assert len(result) > 0
        assert result['clicked'].dtype == int
        assert (result['clicked'].isin([0, 1])).all()
    
    def test_validate_data(self, sample_data):
        """Test para validación de datos."""
        prints, taps, pays = sample_data
        
        # Test con datos válidos
        assert validate_data(prints, taps, pays) == True
        
        # Test con datos inválidos (columna faltante)
        invalid_prints = prints.drop(columns=['user_id'])
        assert validate_data(invalid_prints, taps, pays) == False
    
    def test_calculate_data_quality_metrics(self, sample_data):
        """Test para métricas de calidad de datos."""
        prints, _, _ = sample_data
        
        metrics = calculate_data_quality_metrics(prints)
        
        assert 'total_rows' in metrics
        assert 'total_columns' in metrics
        assert 'missing_values' in metrics
        assert 'duplicate_rows' in metrics
        assert metrics['total_rows'] == len(prints)
        assert metrics['total_columns'] == len(prints.columns)
    
    def test_validate_dataset_schema(self, sample_data):
        """Test para validación de esquema."""
        prints, _, _ = sample_data
        
        expected_schema = {
            'user_id': 'object',
            'value_prop_id': 'object',
            'timestamp': 'datetime64'
        }
        
        assert validate_dataset_schema(prints, expected_schema) == True
        
        # Test con esquema inválido
        invalid_schema = {
            'user_id': 'object',
            'nonexistent_column': 'object'
        }
        assert validate_dataset_schema(prints, invalid_schema) == False

class TestIntegration:
    """Tests de integración."""
    
    @patch('src.io_utils.load_data')
    def test_build_dataset_integration(self, mock_load_data, sample_data):
        """Test de integración para build_dataset."""
        mock_load_data.return_value = sample_data
        
        result = build_dataset()
        
        assert len(result) > 0
        expected_columns = [
            'user_id', 'value_prop_id', 'timestamp', 'clicked',
            'print_count_3w', 'tap_count_3w', 'pay_count_3w', 'total_amount_3w'
        ]
        
        for col in expected_columns:
            assert col in result.columns
    
    def test_pipeline_with_validation(self, sample_data):
        """Test del pipeline con validaciones."""
        with patch('src.io_utils.load_data', return_value=sample_data):
            result = run_pipeline_with_validation()
            
            assert len(result) > 0
            assert 'clicked' in result.columns
            assert (result['clicked'].isin([0, 1])).all()

class TestEdgeCases:
    """Tests para casos edge."""
    
    def test_empty_dataframes(self):
        """Test con DataFrames vacíos."""
        empty_df = pd.DataFrame()
        
        # Test con DataFrames vacíos
        with pytest.raises(Exception):
            add_click_flag(empty_df, empty_df)
    
    def test_missing_columns(self):
        """Test con columnas faltantes."""
        df = pd.DataFrame({'user_id': ['user1']})
        
        with pytest.raises(KeyError):
            add_click_flag(df, df)
    
    def test_invalid_date_ranges(self):
        """Test con rangos de fechas inválidos."""
        df = pd.DataFrame({
            'user_id': ['user1'],
            'value_prop_id': ['prop1'],
            'timestamp': [datetime.now()]
        })
        
        # Fecha final antes que fecha inicial
        end_date = datetime.now()
        start_date = end_date + timedelta(days=1)
        
        result = add_historical_features(
            df, df, start_date, end_date,
            {'test_feature': {'agg_func': 'count'}}
        )
        
        assert 'test_feature' in result.columns
        assert (result['test_feature'] == 0).all()

if __name__ == "__main__":
    pytest.main([__file__]) 