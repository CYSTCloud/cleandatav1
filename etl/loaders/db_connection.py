#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Module de gestion des connexions à la base de données
"""

import mysql.connector
from mysql.connector import Error

class DBConnection:
    """Classe responsable de la gestion des connexions à la base de données"""
    
    def __init__(self, db_config):
        """
        Initialise la connexion à la base de données
        
        Args:
            db_config (dict): Configuration de la base de données
        """
        self.db_config = db_config
        self.conn = None
        self.cursor = None
    
    def connect(self):
        """
        Établit la connexion à la base de données
        
        Returns:
            bool: True si la connexion a réussi, False sinon
        """
        try:
            self.conn = mysql.connector.connect(**self.db_config)
            if self.conn.is_connected():
                self.cursor = self.conn.cursor()
                print("Connexion à la base de données MySQL établie")
                return True
            return False
        except Error as e:
            print(f"Erreur lors de la connexion à MySQL: {e}")
            return False
    
    def disconnect(self):
        """Ferme la connexion à la base de données"""
        if self.cursor:
            self.cursor.close()
        if self.conn and self.conn.is_connected():
            self.conn.close()
            print("Connexion à MySQL fermée")
    
    def verify_table_structure(self, table_name):
        """
        Vérifie la structure d'une table
        
        Args:
            table_name (str): Nom de la table à vérifier
            
        Returns:
            list: Liste des colonnes de la table
        """
        try:
            self.cursor.execute(f"DESCRIBE {table_name}")
            columns = self.cursor.fetchall()
            print(f"Structure de la table {table_name}:")
            for column in columns:
                print(f"  {column[0]} - {column[1]}")
            return columns
        except Error as e:
            print(f"Erreur lors de la vérification de la structure de {table_name}: {e}")
            return []
    
    def truncate_tables(self, tables):
        """
        Vide les tables spécifiées
        
        Args:
            tables (list): Liste des noms de tables à vider
            
        Returns:
            bool: True si l'opération a réussi, False sinon
        """
        try:
            self.cursor.execute("SET FOREIGN_KEY_CHECKS = 0")
            for table in reversed(tables):
                self.cursor.execute(f"TRUNCATE TABLE {table}")
                print(f"Table {table} vidée")
            self.cursor.execute("SET FOREIGN_KEY_CHECKS = 1")
            return True
        except Error as e:
            print(f"Erreur lors du vidage des tables: {e}")
            return False
    
    def count_rows(self, table_name):
        """
        Compte le nombre de lignes dans une table
        
        Args:
            table_name (str): Nom de la table
            
        Returns:
            int: Nombre de lignes dans la table
        """
        try:
            self.cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            count = self.cursor.fetchone()[0]
            return count
        except Error as e:
            print(f"Erreur lors du comptage des lignes dans {table_name}: {e}")
            return 0
