#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Module de chargement des données vers des fichiers CSV
"""

import os
import pandas as pd

class CSVLoader:
    """Classe responsable du chargement des données vers des fichiers CSV"""
    
    @staticmethod
    def save_to_csv(df, output_path, index=False):
        """
        Sauvegarde un DataFrame dans un fichier CSV
        
        Args:
            df (DataFrame): DataFrame pandas à sauvegarder
            output_path (str): Chemin du fichier CSV de sortie
            index (bool): Indique si l'index doit être inclus
            
        Returns:
            bool: True si la sauvegarde a réussi, False sinon
        """
        try:
            # Création du répertoire parent si nécessaire
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            
            # Sauvegarde du DataFrame
            df.to_csv(output_path, index=index)
            print(f"Sauvegarde réussie: {output_path}, {len(df)} lignes")
            return True
        except Exception as e:
            print(f"Erreur lors de la sauvegarde de {output_path}: {e}")
            return False
    
    @staticmethod
    def save_tables_to_csv(tables_dict, output_dir='./processed'):
        """
        Sauvegarde plusieurs DataFrames dans des fichiers CSV
        
        Args:
            tables_dict (dict): Dictionnaire de DataFrames à sauvegarder
            output_dir (str): Répertoire de sortie
            
        Returns:
            dict: Dictionnaire des chemins de fichiers sauvegardés
        """
        # Création du répertoire de sortie si nécessaire
        os.makedirs(output_dir, exist_ok=True)
        
        # Sauvegarde de chaque DataFrame
        output_paths = {}
        for table_name, df in tables_dict.items():
            output_path = os.path.join(output_dir, f"sql_{table_name}.csv")
            if CSVLoader.save_to_csv(df, output_path):
                output_paths[table_name] = output_path
        
        return output_paths
