#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Module de préparation des données selon le schéma SQL
"""

import pandas as pd
from etl.transformers.reference_tables import CalendrierTransformer, LocalisationTransformer, PandemieTransformer
from etl.transformers.data_table import DataTableTransformer

class SchemaTransformer:
    """Classe responsable de la préparation des données selon le schéma SQL"""
    
    def __init__(self):
        """Initialise le transformateur de schéma"""
        self.tables = {}
    
    def prepare_tables(self, dataframes):
        """
        Prépare toutes les tables selon le schéma SQL
        
        Args:
            dataframes (list): Liste de tuples (nom, DataFrame)
            
        Returns:
            dict: Dictionnaire des DataFrames préparés
        """
        # Préparation des tables de référence
        self.tables['calendar'] = CalendrierTransformer.prepare(dataframes)
        self.tables['location'] = LocalisationTransformer.prepare(dataframes)
        self.tables['pandemie'] = PandemieTransformer.prepare()
        
        # Préparation de la table de données
        self.tables['data'] = DataTableTransformer.prepare(
            dataframes, 
            self.tables['calendar'],
            self.tables['location'],
            self.tables['pandemie']
        )
        
        # Affichage des statistiques
        self._print_stats()
        
        return self.tables
    
    def _print_stats(self):
        """Affiche les statistiques des tables préparées"""
        print("\nStatistiques des tables préparées:")
        for table_name, df in self.tables.items():
            print(f"Table {table_name}: {len(df)} lignes")
