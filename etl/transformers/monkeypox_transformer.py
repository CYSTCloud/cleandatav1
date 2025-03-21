#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Module de transformation des données de la variole du singe (Monkeypox)
"""

import pandas as pd
import numpy as np

class MonkeypoxTransformer:
    """Classe responsable de la transformation des données de la variole du singe"""
    
    @staticmethod
    def transform_monkeypox_data(df):
        """
        Transforme les données du fichier owid-monkeypox-data.csv
        
        Args:
            df (DataFrame): DataFrame contenant les données brutes
            
        Returns:
            DataFrame: DataFrame transformé
        """
        # Copie du DataFrame pour éviter de modifier l'original
        df_transformed = df.copy()
        
        # Conversion des colonnes de dates
        df_transformed['date'] = pd.to_datetime(df_transformed['date'])
        
        # Renommage des colonnes pour correspondre au format standard
        df_transformed = df_transformed.rename(columns={
            'location': 'Country/Region',
            'date': 'Date',
            'total_cases': 'Confirmed',
            'total_deaths': 'Deaths'
        })
        
        # Remplacement des valeurs manquantes par 0
        for col in ['Confirmed', 'Deaths']:
            df_transformed[col] = df_transformed[col].fillna(0).astype(int)
        
        # Ajout des colonnes manquantes
        df_transformed['Recovered'] = 0
        df_transformed['Active'] = df_transformed['Confirmed'] - df_transformed['Deaths']
        
        print(f"Transformation Monkeypox: {len(df_transformed)} lignes")
        return df_transformed
