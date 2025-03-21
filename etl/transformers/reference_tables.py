#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Module de préparation des tables de référence
"""

import pandas as pd
from datetime import datetime

class CalendrierTransformer:
    """Classe responsable de la préparation de la table calendar"""
    
    @staticmethod
    def prepare(dataframes):
        """
        Prépare les données pour la table calendar
        
        Args:
            dataframes (list): Liste de tuples (nom, DataFrame)
            
        Returns:
            DataFrame: DataFrame pour la table calendar
        """
        # Extraction des dates uniques
        all_dates = []
        
        for df_name, df in dataframes:
            if 'Date' in df.columns:
                all_dates.extend(df['Date'].dt.date.unique())
            elif 'date' in df.columns:
                all_dates.extend(df['date'].dt.date.unique())
        
        # Suppression des doublons
        unique_dates = sorted(list(set(all_dates)))
        
        # Création du DataFrame calendar
        calendar_data = []
        for i, date in enumerate(unique_dates, start=1):
            date_obj = pd.Timestamp(date)
            # Convertir la date en entier au format YYYYMMDD
            date_value = int(date_obj.strftime('%Y%m%d'))
            calendar_data.append({
                'id': i,
                'date_value': date_value
            })
        
        df_calendar = pd.DataFrame(calendar_data)
        print(f"Préparation table calendar réussie: {len(df_calendar)} lignes")
        return df_calendar

class LocalisationTransformer:
    """Classe responsable de la préparation de la table location"""
    
    @staticmethod
    def prepare(dataframes):
        """
        Prépare les données pour la table location
        
        Args:
            dataframes (list): Liste de tuples (nom, DataFrame)
            
        Returns:
            DataFrame: DataFrame pour la table location
        """
        # Extraction des pays uniques
        all_countries = []
        
        for df_name, df in dataframes:
            if 'Country/Region' in df.columns:
                all_countries.extend(df['Country/Region'].unique())
            elif 'location' in df.columns:
                all_countries.extend(df['location'].unique())
            elif 'country' in df.columns:
                all_countries.extend(df['country'].unique())
        
        # Suppression des doublons
        unique_countries = sorted(list(set([c for c in all_countries if c is not None])))
        
        # Mapping simple des continents
        continent_mapping = {
            'US': 'North America',
            'China': 'Asia',
            'United Kingdom': 'Europe',
            'France': 'Europe',
            'Germany': 'Europe',
            'Italy': 'Europe',
            'Spain': 'Europe',
            'Russia': 'Europe',
            'Brazil': 'South America',
            'India': 'Asia',
            'Japan': 'Asia',
            'South Korea': 'Asia',
            'Australia': 'Oceania',
            'Canada': 'North America',
            'Mexico': 'North America',
            'South Africa': 'Africa',
            'Nigeria': 'Africa',
            'Egypt': 'Africa'
        }
        
        # Création du DataFrame location
        location_data = []
        for i, country in enumerate(unique_countries, start=1):
            continent = continent_mapping.get(country, 'Unknown')
            location_data.append({
                'id': i,
                'country': country,
                'continent': continent
            })
        
        df_location = pd.DataFrame(location_data)
        print(f"Préparation table location réussie: {len(df_location)} lignes")
        return df_location

class PandemieTransformer:
    """Classe responsable de la préparation de la table pandemie"""
    
    @staticmethod
    def prepare():
        """
        Prépare les données pour la table pandemie
        
        Returns:
            DataFrame: DataFrame pour la table pandemie
        """
        # Création du DataFrame pandemie
        pandemie_data = [
            {
                'id': 1,
                'type': 'COVID-19'
            },
            {
                'id': 2,
                'type': 'Monkeypox'
            }
        ]
        
        df_pandemie = pd.DataFrame(pandemie_data)
        print(f"Préparation table pandemie réussie: {len(df_pandemie)} lignes")
        return df_pandemie
