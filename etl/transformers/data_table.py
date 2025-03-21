#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Module de préparation de la table de données principale
"""

import pandas as pd
import numpy as np

class DataTableTransformer:
    """Classe responsable de la préparation de la table data"""
    
    @staticmethod
    def prepare(dataframes, df_calendar, df_location, df_pandemie):
        """
        Prépare les données pour la table data
        
        Args:
            dataframes (list): Liste de tuples (nom, DataFrame)
            df_calendar (DataFrame): DataFrame de la table calendar
            df_location (DataFrame): DataFrame de la table location
            df_pandemie (DataFrame): DataFrame de la table pandemie
            
        Returns:
            DataFrame: DataFrame pour la table data
        """
        # Création des dictionnaires pour les lookups
        date_to_id = dict(zip(df_calendar['date_value'], df_calendar['id']))
        country_to_id = dict(zip(df_location['country'], df_location['id']))
        pandemie_to_id = dict(zip(df_pandemie['type'], df_pandemie['id']))
        
        # Préparation des données
        data_rows = []
        id_counter = 1
        
        for df_name, df in dataframes:
            # Détermination du type de pandémie
            pandemie_id = 1  # COVID-19 par défaut
            if 'monkeypox' in df_name.lower():
                pandemie_id = 2  # Monkeypox
            
            # Traitement des données selon le format du DataFrame
            data_rows.extend(
                DataTableTransformer._process_dataframe(
                    df, df_name, pandemie_id, date_to_id, country_to_id, id_counter
                )
            )
            id_counter += len(data_rows)
        
        # Création du DataFrame data
        if data_rows:
            df_data = pd.DataFrame(data_rows)
            print(f"Préparation table data réussie: {len(df_data)} lignes")
            return df_data
        else:
            print("Aucune donnée à préparer pour la table data")
            return pd.DataFrame()
    
    @staticmethod
    def _process_dataframe(df, df_name, pandemie_id, date_to_id, country_to_id, start_id):
        """
        Traite un DataFrame pour extraire les données pour la table data
        
        Args:
            df (DataFrame): DataFrame à traiter
            df_name (str): Nom du DataFrame
            pandemie_id (int): ID de la pandémie
            date_to_id (dict): Dictionnaire de mapping date -> id_calendar
            country_to_id (dict): Dictionnaire de mapping pays -> id_location
            start_id (int): ID de départ pour les lignes
            
        Returns:
            list: Liste de dictionnaires pour la table data
        """
        data_rows = []
        id_counter = start_id
        
        # Détermination des colonnes selon le format du DataFrame
        if 'covid_19_clean_complete' in df_name.lower():
            return DataTableTransformer._process_covid_clean(df, pandemie_id, date_to_id, country_to_id, id_counter)
        elif 'monkeypox' in df_name.lower():
            return DataTableTransformer._process_monkeypox(df, pandemie_id, date_to_id, country_to_id, id_counter)
        elif 'worldometer' in df_name.lower():
            return DataTableTransformer._process_worldometer(df, pandemie_id, date_to_id, country_to_id, id_counter)
        
        return data_rows
    
    @staticmethod
    def _process_covid_clean(df, pandemie_id, date_to_id, country_to_id, start_id):
        """Traite les données du fichier covid_19_clean_complete.csv"""
        data_rows = []
        id_counter = start_id
        
        for _, row in df.iterrows():
            try:
                # Conversion de la date au format YYYYMMDD
                date_str = pd.to_datetime(row['Date']).strftime('%Y%m%d')
                date_value = int(date_str)
                
                # Récupération des IDs
                calendar_id = date_to_id.get(date_value)
                location_id = country_to_id.get(row['Country/Region'])
                
                if calendar_id and location_id:
                    # Extraction des métriques
                    total_cases = int(row['Confirmed']) if not pd.isna(row['Confirmed']) else 0
                    total_deaths = int(row['Deaths']) if not pd.isna(row['Deaths']) else 0
                    
                    # Calcul des nouveaux cas/décès (non disponible directement)
                    new_cases = 0
                    new_deaths = 0
                    
                    data_rows.append({
                        'id': id_counter,
                        'total_cases': total_cases,
                        'total_deaths': total_deaths,
                        'new_cases': new_cases,
                        'new_deaths': new_deaths,
                        'id_location': location_id,
                        'id_pandemie': pandemie_id,
                        'id_calendar': calendar_id
                    })
                    
                    id_counter += 1
            except Exception as e:
                print(f"Erreur lors du traitement d'une ligne de covid_19_clean_complete: {e}")
        
        return data_rows
    
    @staticmethod
    def _process_monkeypox(df, pandemie_id, date_to_id, country_to_id, start_id):
        """Traite les données du fichier owid-monkeypox-data.csv"""
        data_rows = []
        id_counter = start_id
        
        for _, row in df.iterrows():
            try:
                # Vérification que la date est une chaîne de caractères
                if not isinstance(row['date'], str):
                    continue
                
                # Conversion de la date au format YYYYMMDD
                date_str = pd.to_datetime(row['date']).strftime('%Y%m%d')
                date_value = int(date_str)
                
                # Récupération des IDs
                calendar_id = date_to_id.get(date_value)
                location_id = country_to_id.get(row['location'])
                
                if calendar_id and location_id:
                    # Extraction des métriques
                    total_cases = int(row['total_cases']) if 'total_cases' in row and not pd.isna(row['total_cases']) else 0
                    total_deaths = int(row['total_deaths']) if 'total_deaths' in row and not pd.isna(row['total_deaths']) else 0
                    new_cases = int(row['new_cases']) if 'new_cases' in row and not pd.isna(row['new_cases']) else 0
                    new_deaths = int(row['new_deaths']) if 'new_deaths' in row and not pd.isna(row['new_deaths']) else 0
                    
                    data_rows.append({
                        'id': id_counter,
                        'total_cases': total_cases,
                        'total_deaths': total_deaths,
                        'new_cases': new_cases,
                        'new_deaths': new_deaths,
                        'id_location': location_id,
                        'id_pandemie': pandemie_id,
                        'id_calendar': calendar_id
                    })
                    
                    id_counter += 1
            except Exception as e:
                # Ignorer les erreurs de conversion de date
                pass
        
        return data_rows
    
    @staticmethod
    def _process_worldometer(df, pandemie_id, date_to_id, country_to_id, start_id):
        """Traite les données du fichier worldometer_coronavirus_daily_data.csv"""
        data_rows = []
        id_counter = start_id
        
        for _, row in df.iterrows():
            try:
                # Vérification que la date est une chaîne de caractères
                if not isinstance(row['date'], str):
                    continue
                
                # Conversion de la date au format YYYYMMDD
                date_str = pd.to_datetime(row['date']).strftime('%Y%m%d')
                date_value = int(date_str)
                
                # Récupération des IDs
                calendar_id = date_to_id.get(date_value)
                location_id = country_to_id.get(row['country'])
                
                if calendar_id and location_id:
                    # Extraction des métriques
                    total_cases = int(row['cumulative_total_cases']) if 'cumulative_total_cases' in row and not pd.isna(row['cumulative_total_cases']) else 0
                    total_deaths = int(row['cumulative_total_deaths']) if 'cumulative_total_deaths' in row and not pd.isna(row['cumulative_total_deaths']) else 0
                    new_cases = int(row['daily_new_cases']) if 'daily_new_cases' in row and not pd.isna(row['daily_new_cases']) else 0
                    new_deaths = int(row['daily_new_deaths']) if 'daily_new_deaths' in row and not pd.isna(row['daily_new_deaths']) else 0
                    
                    data_rows.append({
                        'id': id_counter,
                        'total_cases': total_cases,
                        'total_deaths': total_deaths,
                        'new_cases': new_cases,
                        'new_deaths': new_deaths,
                        'id_location': location_id,
                        'id_pandemie': pandemie_id,
                        'id_calendar': calendar_id
                    })
                    
                    id_counter += 1
            except Exception as e:
                # Ignorer les erreurs de conversion de date
                pass
        
        return data_rows
