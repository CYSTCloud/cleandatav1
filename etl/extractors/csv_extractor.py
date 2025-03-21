#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Module d'extraction des données CSV pour le processus ETL
"""

import os
import pandas as pd

class CSVExtractor:
    """Classe responsable de l'extraction des données à partir de fichiers CSV"""
    
    @staticmethod
    def extract_file(file_path):
        """
        Extrait les données d'un fichier CSV
        
        Args:
            file_path (str): Chemin du fichier CSV à extraire
            
        Returns:
            DataFrame: DataFrame pandas contenant les données extraites
        """
        try:
            df = pd.read_csv(file_path)
            print(f"Extraction réussie: {file_path}, {len(df)} lignes")
            return df
        except Exception as e:
            print(f"Erreur lors de l'extraction de {file_path}: {e}")
            return pd.DataFrame()
    
    def extract_data(self, input_files):
        """
        Extrait les données de plusieurs fichiers CSV
        
        Args:
            input_files (list): Liste des chemins de fichiers CSV
            
        Returns:
            list: Liste de tuples (nom_fichier, dataframe)
        """
        dataframes = []
        
        for file_path in input_files:
            file_name = os.path.basename(file_path)
            df = self.extract_file(file_path)
            
            if not df.empty:
                dataframes.append((file_name, df))
        
        return dataframes
    
    @staticmethod
    def extract_covid_clean_complete(input_dir='.'):
        """
        Extrait les données du fichier covid_19_clean_complete.csv
        
        Args:
            input_dir (str): Répertoire contenant le fichier
            
        Returns:
            DataFrame: DataFrame pandas contenant les données extraites
        """
        file_path = os.path.join(input_dir, 'covid_19_clean_complete.csv')
        return CSVExtractor.extract_file(file_path)
    
    @staticmethod
    def extract_monkeypox_data(input_dir='.'):
        """
        Extrait les données du fichier owid-monkeypox-data.csv
        
        Args:
            input_dir (str): Répertoire contenant le fichier
            
        Returns:
            DataFrame: DataFrame pandas contenant les données extraites
        """
        file_path = os.path.join(input_dir, 'owid-monkeypox-data.csv')
        return CSVExtractor.extract_file(file_path)
    
    @staticmethod
    def extract_worldometer_covid(input_dir='.'):
        """
        Extrait les données du fichier worldometer_coronavirus_daily_data.csv
        
        Args:
            input_dir (str): Répertoire contenant le fichier
            
        Returns:
            DataFrame: DataFrame pandas contenant les données extraites
        """
        file_path = os.path.join(input_dir, 'worldometer_coronavirus_daily_data.csv')
        return CSVExtractor.extract_file(file_path)
