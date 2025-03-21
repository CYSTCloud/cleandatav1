#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Module de chargement des données vers une base de données MySQL
"""

import pandas as pd
from etl.loaders.db_connection import DBConnection
from etl.loaders.table_loaders import CalendrierLoader, LocalisationLoader, PandemieLoader, DataLoader

class DBLoader:
    """Classe responsable du chargement des données vers une base de données MySQL"""
    
    def __init__(self, db_config):
        """
        Initialise le chargeur de base de données
        
        Args:
            db_config (dict): Configuration de la base de données
        """
        self.db_config = db_config
        self.connection = DBConnection(db_config)
    
    def load_data(self, tables_dict):
        """
        Charge les données dans la base de données
        
        Args:
            tables_dict (dict): Dictionnaire contenant les DataFrames à charger
            
        Returns:
            dict: Dictionnaire des nombres de lignes chargées par table
        """
        results = {}
        
        # Connexion à la base de données
        if not self.connection.connect():
            return results
        
        try:
            # Vérification de la structure des tables
            tables_list = list(tables_dict.keys())
            for table in tables_list:
                self.connection.verify_table_structure(table)
            
            # Vidage des tables
            self.connection.truncate_tables(tables_list)
            
            # Importation des données
            if 'calendar' in tables_dict:
                results['calendar'] = CalendrierLoader.import_data(
                    self.connection, tables_dict['calendar'])
            
            if 'location' in tables_dict:
                results['location'] = LocalisationLoader.import_data(
                    self.connection, tables_dict['location'])
            
            if 'pandemie' in tables_dict:
                results['pandemie'] = PandemieLoader.import_data(
                    self.connection, tables_dict['pandemie'])
            
            if 'data' in tables_dict:
                results['data'] = DataLoader.import_data(
                    self.connection, tables_dict['data'])
            
            # Vérification du nombre de lignes
            self.verify_row_counts(tables_list)
            
        finally:
            # Fermeture de la connexion
            self.connection.disconnect()
        
        return results
    
    def verify_row_counts(self, tables):
        """
        Vérifie le nombre de lignes dans chaque table
        
        Args:
            tables (list): Liste des noms de tables à vérifier
            
        Returns:
            dict: Dictionnaire des nombres de lignes par table
        """
        row_counts = {}
        print("Vérification du nombre de lignes dans chaque table:")
        
        for table in tables:
            count = self.connection.count_rows(table)
            print(f"Table {table}: {count} lignes")
            row_counts[table] = count
        
        return row_counts
