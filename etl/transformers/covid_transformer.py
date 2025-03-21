#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Module de transformation des données COVID-19
"""

import pandas as pd
import numpy as np

class CovidTransformer:
    """Classe responsable de la transformation des données COVID-19"""
    
    @staticmethod
    def transform_covid_clean_complete(df):
        """
        Transforme les données du fichier covid_19_clean_complete.csv
        
        Args:
            df (DataFrame): DataFrame contenant les données brutes
            
        Returns:
            DataFrame: DataFrame transformé
        """
        # Copie du DataFrame pour éviter de modifier l'original
        df_transformed = df.copy()
        
        # Conversion des colonnes de dates
        df_transformed['Date'] = pd.to_datetime(df_transformed['Date'])
        
        # Remplacement des valeurs manquantes par 0
        for col in ['Confirmed', 'Deaths', 'Recovered', 'Active']:
            df_transformed[col] = df_transformed[col].fillna(0).astype(int)
        
        # Agrégation par pays et date
        df_agg = df_transformed.groupby(['Country/Region', 'Date']).agg({
            'Confirmed': 'sum',
            'Deaths': 'sum',
            'Recovered': 'sum',
            'Active': 'sum'
        }).reset_index()
        
        print(f"Transformation COVID Clean Complete: {len(df_agg)} lignes")
        return df_agg
    
    @staticmethod
    def transform_worldometer_covid(df):
        """
        Transforme les données du fichier worldometer_coronavirus_daily_data.csv
        
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
            'country': 'Country/Region',
            'date': 'Date',
            'cumulative_total_cases': 'Confirmed',
            'cumulative_total_deaths': 'Deaths'
        })
        
        # Ajout des colonnes manquantes
        df_transformed['Recovered'] = 0
        df_transformed['Active'] = df_transformed['Confirmed'] - df_transformed['Deaths']
        
        print(f"Transformation Worldometer COVID: {len(df_transformed)} lignes")
        return df_transformed
