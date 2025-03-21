#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Module de transformation des données brutes
"""

import pandas as pd
from etl.transformers.covid_transformer import CovidTransformer
from etl.transformers.monkeypox_transformer import MonkeypoxTransformer

class DataTransformer:
    """Classe responsable de la transformation des données brutes"""
    
    def __init__(self):
        """Initialise le transformateur de données"""
        self.transformers = {
            'covid_19_clean_complete.csv': CovidTransformer.transform_covid_clean_complete,
            'worldometer_coronavirus_daily_data.csv': CovidTransformer.transform_worldometer_covid,
            'owid-monkeypox-data.csv': MonkeypoxTransformer.transform_monkeypox_data
        }
    
    def transform_data(self, dataframes):
        """
        Transforme les données brutes
        
        Args:
            dataframes (list): Liste de tuples (nom, DataFrame)
            
        Returns:
            list: Liste de tuples (nom, DataFrame transformé)
        """
        transformed_dataframes = []
        
        for df_name, df in dataframes:
            # Recherche du transformateur approprié
            transformer_func = self._get_transformer(df_name)
            
            if transformer_func:
                # Transformation des données
                transformed_df = transformer_func(df)
                transformed_dataframes.append((df_name, transformed_df))
                print(f"Transformation réussie pour {df_name}")
            else:
                # Aucun transformateur trouvé, utilisation des données brutes
                print(f"Aucun transformateur trouvé pour {df_name}, utilisation des données brutes")
                transformed_dataframes.append((df_name, df))
        
        return transformed_dataframes
    
    def _get_transformer(self, df_name):
        """
        Récupère la fonction de transformation appropriée pour un fichier
        
        Args:
            df_name (str): Nom du fichier
            
        Returns:
            function: Fonction de transformation ou None si aucune n'est trouvée
        """
        # Recherche directe par nom de fichier
        if df_name in self.transformers:
            return self.transformers[df_name]
        
        # Recherche par correspondance partielle
        for pattern, transformer in self.transformers.items():
            if pattern in df_name:
                return transformer
        
        return None
