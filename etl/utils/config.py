#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Module de configuration pour le processus ETL
"""

import os
import json
from pathlib import Path

class Config:
    """Classe de gestion de la configuration"""
    
    @staticmethod
    def get_default_db_config():
        """
        Retourne la configuration par défaut pour la base de données
        
        Returns:
            dict: Configuration par défaut
        """
        return {
            'host': 'localhost',
            'user': 'root',
            'password': '',  # Modifiez si vous avez un mot de passe
            'database': 'epiviz'
        }
    
    @staticmethod
    def get_default_paths():
        """
        Retourne les chemins par défaut pour les fichiers
        
        Returns:
            dict: Chemins par défaut
        """
        base_dir = Path('.')
        return {
            'input_dir': str(base_dir),
            'output_dir': str(base_dir / 'processed'),
            'covid_clean_file': str(base_dir / 'covid_19_clean_complete.csv'),
            'monkeypox_file': str(base_dir / 'owid-monkeypox-data.csv'),
            'worldometer_file': str(base_dir / 'worldometer_coronavirus_daily_data.csv')
        }
    
    @staticmethod
    def load_config(config_file='config.json'):
        """
        Charge la configuration depuis un fichier JSON
        
        Args:
            config_file (str): Chemin du fichier de configuration
            
        Returns:
            dict: Configuration chargée
        """
        try:
            if os.path.exists(config_file):
                with open(config_file, 'r') as f:
                    return json.load(f)
            else:
                print(f"Fichier de configuration {config_file} non trouvé, utilisation des valeurs par défaut")
                return {
                    'db': Config.get_default_db_config(),
                    'paths': Config.get_default_paths()
                }
        except Exception as e:
            print(f"Erreur lors du chargement de la configuration: {e}")
            return {
                'db': Config.get_default_db_config(),
                'paths': Config.get_default_paths()
            }
    
    @staticmethod
    def save_config(config, config_file='config.json'):
        """
        Sauvegarde la configuration dans un fichier JSON
        
        Args:
            config (dict): Configuration à sauvegarder
            config_file (str): Chemin du fichier de configuration
            
        Returns:
            bool: True si la sauvegarde a réussi, False sinon
        """
        try:
            with open(config_file, 'w') as f:
                json.dump(config, f, indent=4)
            print(f"Configuration sauvegardée dans {config_file}")
            return True
        except Exception as e:
            print(f"Erreur lors de la sauvegarde de la configuration: {e}")
            return False
