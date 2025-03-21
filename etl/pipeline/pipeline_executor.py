#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Module d'exécution du pipeline ETL
"""

import os
import pandas as pd

class PipelineExecutor:
    """Classe responsable de l'exécution du pipeline ETL"""
    
    def __init__(self, extractor, transformer, schema_transformer, csv_loader, db_loader=None):
        """
        Initialise l'exécuteur du pipeline
        
        Args:
            extractor: Extracteur de données
            transformer: Transformateur de données
            schema_transformer: Transformateur de schéma
            csv_loader: Chargeur de fichiers CSV
            db_loader: Chargeur de base de données (optionnel)
        """
        self.extractor = extractor
        self.transformer = transformer
        self.schema_transformer = schema_transformer
        self.csv_loader = csv_loader
        self.db_loader = db_loader
    
    def run(self, input_files, output_dir, load_to_db=False):
        """
        Exécute le pipeline ETL
        
        Args:
            input_files (list): Liste des fichiers d'entrée
            output_dir (str): Répertoire de sortie
            load_to_db (bool): Indique si les données doivent être chargées dans la base de données
            
        Returns:
            dict: Résultats de l'exécution
        """
        results = {
            'extraction': 0,
            'transformation': 0,
            'schema': {},
            'csv_loading': {},
            'db_loading': {}
        }
        
        # Étape 1: Extraction
        print("\n=== ÉTAPE 1: EXTRACTION ===")
        raw_dataframes = self.extractor.extract_data(input_files)
        results['extraction'] = sum(len(df) for _, df in raw_dataframes)
        
        # Étape 2: Transformation
        print("\n=== ÉTAPE 2: TRANSFORMATION ===")
        transformed_dataframes = self.transformer.transform_data(raw_dataframes)
        results['transformation'] = sum(len(df) for _, df in transformed_dataframes)
        
        # Étape 3: Préparation selon le schéma SQL
        print("\n=== ÉTAPE 3: PRÉPARATION SELON LE SCHÉMA SQL ===")
        tables = self.schema_transformer.prepare_tables(transformed_dataframes)
        results['schema'] = {table: len(df) for table, df in tables.items()}
        
        # Étape 4: Chargement dans des fichiers CSV
        print("\n=== ÉTAPE 4: CHARGEMENT DANS DES FICHIERS CSV ===")
        csv_results = self.csv_loader.save_tables_to_csv(tables, output_dir)
        results['csv_loading'] = {table: len(tables[table]) for table in csv_results.keys()}
        
        # Étape 5: Chargement dans la base de données (optionnel)
        if load_to_db and self.db_loader:
            print("\n=== ÉTAPE 5: CHARGEMENT DANS LA BASE DE DONNÉES ===")
            db_results = self.db_loader.load_data(tables)
            results['db_loading'] = db_results
        
        return results
