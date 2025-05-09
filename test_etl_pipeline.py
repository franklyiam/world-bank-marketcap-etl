"""Unit tests for the ETL pipeline for World Bank Market Capitalization"""

import unittest
import pandas as pd
import sqlite3
from bank_etl_project import extract, transform, load_to_csv, load_to_db

class TestETLPipeline(unittest.TestCase):
    """Test suite for ETL functions"""

    def setUp(self):
        """Set up test variables and sample data"""
        self.url = 'https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks'
        self.table_attribs = ["Name", "MC_USD_Billion"]
        self.exchange_path = 'exchange_rate.csv'
        self.test_csv = 'test_output.csv'
        self.test_db = 'test_banks.db'
        self.table_name = 'Largest_banks'

    def test_extract(self):
        """Test if extract function returns a non-empty DataFrame"""
        df = extract(self.url, self.table_attribs)
        self.assertFalse(df.empty, "Extracted DataFrame should not be empty")
        self.assertIn("Name", df.columns)
        self.assertIn("MC_USD_Billion", df.columns)

    def test_transform(self):
        """Test transformation adds correct currency columns"""
        df = extract(self.url, self.table_attribs)
        transformed_df = transform(df, self.exchange_path)
        self.assertIn("MC_GBP_Billion", transformed_df.columns)
        self.assertIn("MC_EUR_Billion", transformed_df.columns)
        self.assertIn("MC_INR_Billion", transformed_df.columns)

    def test_load_to_csv(self):
        """Test if data can be written to CSV without error"""
        df = extract(self.url, self.table_attribs)
        df = transform(df, self.exchange_path)
        load_to_csv(df, self.test_csv)
        loaded_df = pd.read_csv(self.test_csv)
        self.assertFalse(loaded_df.empty, "CSV file should be created and non-empty")

    def test_load_to_db(self):
        """Test if data can be loaded to database and queried"""
        df = extract(self.url, self.table_attribs)
        df = transform(df, self.exchange_path)
        connection = sqlite3.connect(self.test_db)
        load_to_db(df, connection, self.table_name)
        query_result = pd.read_sql(f"SELECT * FROM {self.table_name}", connection)
        self.assertFalse(query_result.empty, "Database table should contain data")
        connection.close()

if __name__ == '__main__':
    unittest.main()
