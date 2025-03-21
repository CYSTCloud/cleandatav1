#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Module de chargement des données dans les tables spécifiques
"""

from mysql.connector import Error

class CalendrierLoader:
    """Classe responsable du chargement des données dans la table calendar"""
    
    @staticmethod
    def import_data(db_connection, df_calendar):
        """
        Importe les données dans la table calendar
        
        Args:
            db_connection (DBConnection): Connexion à la base de données
            df_calendar (DataFrame): DataFrame contenant les données
            
        Returns:
            int: Nombre de lignes importées
        """
        try:
            for _, row in df_calendar.iterrows():
                query = "INSERT INTO calendar (id, date_value) VALUES (%s, %s)"
                values = (int(row['id']), row['date_value'])
                db_connection.cursor.execute(query, values)
            
            db_connection.conn.commit()
            count = len(df_calendar)
            print(f"{count} lignes importées dans calendar")
            return count
        except Error as e:
            print(f"Erreur lors de l'importation dans calendar: {e}")
            return 0

class LocalisationLoader:
    """Classe responsable du chargement des données dans la table location"""
    
    @staticmethod
    def import_data(db_connection, df_location):
        """
        Importe les données dans la table location
        
        Args:
            db_connection (DBConnection): Connexion à la base de données
            df_location (DataFrame): DataFrame contenant les données
            
        Returns:
            int: Nombre de lignes importées
        """
        try:
            for _, row in df_location.iterrows():
                query = "INSERT INTO location (id, country, continent) VALUES (%s, %s, %s)"
                values = (int(row['id']), row['country'], row['continent'])
                db_connection.cursor.execute(query, values)
            
            db_connection.conn.commit()
            count = len(df_location)
            print(f"{count} lignes importées dans location")
            return count
        except Error as e:
            print(f"Erreur lors de l'importation dans location: {e}")
            return 0

class PandemieLoader:
    """Classe responsable du chargement des données dans la table pandemie"""
    
    @staticmethod
    def import_data(db_connection, df_pandemie):
        """
        Importe les données dans la table pandemie
        
        Args:
            db_connection (DBConnection): Connexion à la base de données
            df_pandemie (DataFrame): DataFrame contenant les données
            
        Returns:
            int: Nombre de lignes importées
        """
        try:
            for _, row in df_pandemie.iterrows():
                query = "INSERT INTO pandemie (id, type) VALUES (%s, %s)"
                values = (int(row['id']), row['type'])
                db_connection.cursor.execute(query, values)
            
            db_connection.conn.commit()
            count = len(df_pandemie)
            print(f"{count} lignes importées dans pandemie")
            return count
        except Error as e:
            print(f"Erreur lors de l'importation dans pandemie: {e}")
            return 0

class DataLoader:
    """Classe responsable du chargement des données dans la table data"""
    
    @staticmethod
    def import_data(db_connection, df_data, batch_size=1000):
        """
        Importe les données dans la table data
        
        Args:
            db_connection (DBConnection): Connexion à la base de données
            df_data (DataFrame): DataFrame contenant les données
            batch_size (int): Taille des lots pour l'importation
            
        Returns:
            int: Nombre de lignes importées
        """
        try:
            total_rows = len(df_data)
            
            for i in range(0, total_rows, batch_size):
                batch = df_data.iloc[i:i+batch_size]
                values_list = []
                
                for _, row in batch.iterrows():
                    values = (
                        int(row['id']),
                        int(row['total_cases']),
                        int(row['total_deaths']),
                        int(row['new_cases']),
                        int(row['new_deaths']),
                        int(row['id_location']),
                        int(row['id_pandemie']),
                        int(row['id_calendar'])
                    )
                    values_list.append(values)
                
                query = """
                INSERT INTO data (id, total_cases, total_deaths, new_cases, new_deaths, 
                                 id_location, id_pandemie, id_calendar)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """
                db_connection.cursor.executemany(query, values_list)
                db_connection.conn.commit()
                print(f"Lot {i//batch_size + 1}/{(total_rows-1)//batch_size + 1} importé ({len(batch)} lignes)")
            
            print(f"{total_rows} lignes importées dans data")
            return total_rows
        except Error as e:
            print(f"Erreur lors de l'importation dans data: {e}")
            return 0
