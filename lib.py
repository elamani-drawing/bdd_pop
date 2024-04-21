import psycopg2
import psycopg2.extras
import pandas as pd
import io 
from requete import *
from config import connexion

def get_connection(verbose=True):
    return login_bdd(connexion["dbname"],connexion["username"],connexion["password"],connexion["host"], connexion["port"], verbose)

def login_bdd(dbname, username, password, host="localhost", port ="5432", verbose = True):
    """
    Permet de réccuperer une connexion à la base de donnée
    """
    if verbose: print('Tentative de connexion à la base de données...')
    try:
        conn = psycopg2.connect(host=host, user = username, password=password,dbname=dbname, port = port) 
        if verbose : print('Connection à la base de donnée réussi.')
        return conn
    except Exception as e :
        exit("Connexion impossible à la base de données: " + str(e))


def execute_sql_file(connection, filename, verbose = True):
    """
    Execute un fichier SQL.
    """
    try: 
        with open(filename, 'r') as f: 
            sql = f.read()
            with connection.cursor() as cursor: 
                cursor.execute(sql) 
                connection.commit() 
        if verbose : print(f"Fichier {filename} exécuté avec succès.")
        cursor.close()
    except Exception as e: 
        connection.rollback()
        if verbose: print(f"Erreur lors de l'exécution du fichier {filename}: {e}")
        raise e


def reset_bdd(connection, params, force) :
    """
    Réenitialise la base de données
    REMET LA BASE DE DONNEES A SON ETAT D'ORIGINE
    """
    if force != "RESET":
        print("Vous devez remplir l'argument force.")
        return False
    # supprime les tables
    execute_sql_file(connection, params["sql_requete"]["delete_sql"])
    # créer les tables
    execute_sql_file(connection, params["sql_requete"]["create_sql"])
    return True


def execute_view_bdd(connection, params) :
    """
    Créer les view indiqué à la question 2
    """
    execute_sql_file(connection, params["sql_requete"]["view_sql"])

def execute_alter_procedure(connection, params) :
    """
    Alter les tables regions, departement pour ajouter la colonne population, et execute une procedure pour avoir les données, comme demander a la question 3
    """
    execute_sql_file(connection, params["sql_requete"]["alter_sql"])
    execute_sql_file(connection, params["sql_requete"]["procedure_sql"])
    cursor = connection.cursor()
    cursor.execute("CALL Update_Population_Departments_And_Regions()") 
    connection.commit()
    cursor.close()
    return True



def insert_regions(connection, file_path, delimiter =","):
    """
    Insère les données des régions à partir d'un fichier dans la table Region.
    Return: Un dataframe contenant les regions et leur chef lieu.
    """
    cursor = connection.cursor()
    try:
        # Charge le fichier CSV dans un DataFrame
        dfr = pd.read_csv(file_path)

        # Sélectionne uniquement les colonnes qui nous interesse
        df = dfr[['REG', 'NCC']]
        df.columns = ['reg', 'ncc']

        # Retire les doublons par rapport à la colonne 'reg'
        df = df.drop_duplicates(subset=['reg'])

        csv_data = df.to_csv(index=False, sep=delimiter, header=False)

        # Crée un flux de texte en mémoire
        csv_io = io.StringIO()
        csv_io.write(csv_data)
        csv_io.seek(0)

        # Insere les données
        cursor.copy_from(csv_io, 'region', sep=delimiter) 

        connection.commit() 
        cursor.close()  

        df_cheflieu =dfr[['REG', 'CHEFLIEU']]
        df_cheflieu.columns = ['id_reg', 'id_com']
        return df_cheflieu
    
    except Exception as e:
        connection.rollback()
        raise e

def insert_departements(connection, file_path, delimiter=","):
    """
    Insère les données des départements à partir d'un fichier dans la table Departement.
    Return: Un dataframe contenant les departement et leur chef lieu.
    """
    cursor = connection.cursor()
    try:
        # Charge le fichier CSV dans un DataFrame
        dfd = pd.read_csv(file_path, delimiter=delimiter) 
        # Sélectionne uniquement les colonnes nécessaires et les renomme
        df = dfd[['DEP', 'NCC', 'REG']]
        df.columns = ['dep', 'ncc', 'id_reg']

        # Retire les doublons par rapport à la colonne 'dep'
        df = df.drop_duplicates(subset=['dep'])

        # Insère les données dans la table Departement
        csv_data = df.to_csv(index=False, sep=delimiter, header=False)

        # Crée un flux de texte en mémoire
        csv_io = io.StringIO()
        csv_io.write(csv_data)
        csv_io.seek(0)

        # Insère les données dans la table Departement
        cursor.copy_from(csv_io, 'departement', sep=delimiter)
        
        connection.commit() 
        cursor.close()  
    
        # Insere les chefs lieu des départements

        df_cheflieu =dfd[['DEP', 'CHEFLIEU']]
        df_cheflieu.columns = ['id_dep', 'id_com']
        return df_cheflieu

    except Exception as e:
        connection.rollback() 
        raise e

def insert_communes(connection, file_path, delimiter=","):
    """
    Insère les données des communes à partir d'un fichier CSV dans la table Commune.
    """
    cursor = connection.cursor()
    try:
        # Charge le fichier CSV dans un DataFrame
        df = pd.read_csv(file_path, delimiter=delimiter)
        
        # Remplace les valeurs manquantes par celles de la ligne précédente
        # Cela fonctionne car les données sont ordonnée par département et région
        df = df.ffill()

        # Sélectionne uniquement les colonnes nécessaires et les renomme
        df = df[['COM', 'NCC', 'DEP']]
        df.columns = ['com', 'ncc', 'id_dep']

        # Retire les doublons par rapport à la colonne 'com'
        df = df.drop_duplicates(subset=['com'])

        # Insère les données dans la table Commune
        csv_data = df.to_csv(index=False, sep=delimiter, header=False)

        # Crée un flux de texte en mémoire
        csv_io = io.StringIO()
        csv_io.write(csv_data)
        csv_io.seek(0)

        # Insère les données dans la table Commune
        cursor.copy_from(csv_io, 'commune', sep=delimiter)
        connection.commit() 
        cursor.close()
    except Exception as e:
        connection.rollback() 
        raise e

def insert_cheflieu(connection, chef_lieu_reg: pd.DataFrame, chef_lieu_dep: pd.DataFrame, delimiter =";"):
    """
    Insert les chef lieu des régions et des départements
    Précondition: Région, Département, Comune doivent avoir été créer et rempli avec les données
    """
    cursor = connection.cursor()
    try   :
        # chef lieu region
        csv_data = chef_lieu_reg.to_csv(index=False, sep=delimiter, header=False)
        # Crée un flux de texte en mémoire pour les chefs lieu des régions
        csv_io_reg = io.StringIO()
        csv_io_reg.write(csv_data)
        csv_io_reg.seek(0)

        # Insere les données
        cursor.copy_from(csv_io_reg, 'cheflieureg', sep=delimiter) 

        # chef lieu departement
        csv_data = chef_lieu_dep.to_csv(index=False, sep=delimiter, header=False)
        # Crée un flux de texte en mémoire pour les chefs lieu des régions
        csv_io_dep = io.StringIO()
        csv_io_dep.write(csv_data)
        csv_io_dep.seek(0)

        # Insere les données
        cursor.copy_from(csv_io_dep, 'cheflieudep', sep=delimiter) 

        
        connection.commit() 
        cursor.close()
    except Exception as e:
        connection.rollback() 
        raise e

def insert_stats_libelle(connection , file_path :str, delimiter=";"):
    """
    Insert les données du fichier dans la table StatsLibelle
    """
    cursor = connection.cursor()
    try:
        # Charger le fichier CSV dans un DataFrame
        df = pd.read_csv(file_path, delimiter=delimiter)

        # Garde uniquement les colonnes spécifiées et les renommer
        df = df[['COD_VAR', 'LIB_VAR']]
        df.columns = ['code', 'libelle']

        # Retire les doublons par rapport à la colonne 'code'
        df = df.drop_duplicates(subset=['code'])

        # Convertit le DataFrame en chaîne CSV
        csv_data = df.to_csv(index=False, sep=delimiter, header=False)

        # Crée un flux de texte en mémoire
        csv_io = io.StringIO()
        csv_io.write(csv_data)
        csv_io.seek(0)

        # Insere les données
        cursor.copy_from(csv_io, 'statslibelle', sep=delimiter) 
        connection.commit() 
        cursor.close()
    except Exception as e:
        connection.rollback() 
        raise e


def insert_stats_population(connection, file_path: str, annee,  first=0, last = None, delimiter=';'):
    """
    Insère les données du fichier CSV dans la table StatsPopulation.
    first et last indique la premiere et la derniere ligne à importer [first, last]
    """
    cursor = connection.cursor()
    try:  
        df = pd.read_csv(file_path, delimiter=delimiter, dtype={'CODGEO': str})

        # Reformate le DataFrame selon les exigences de la table StatsPopulation
        # 'id_com', 'code_stats_libelle', 'annee', 'valeur'
        reformatted_data = []
        for index, row in df.iterrows(): 
            # On recopie les données des qu'on atteint la borne inferieur
            if index >= first :  
                com = row['CODGEO']
                for column in df.columns[1:]:
                    code_stats_libelle = column
                    annee = annee
                    valeur = row[column]
                    reformatted_data.append((com, code_stats_libelle, annee, valeur)) 
                # Arrivé à la borne supperieur on s'arrete
                if last != None and index >= last:
                    break
                
        # Crée un flux de texte en mémoire
        csv_io = io.StringIO()
        for row in reformatted_data:
            csv_io.write(delimiter.join(map(str, row)) + '\n')
        csv_io.seek(0)

        # Insére les données dans la table StatsPopulation
        cursor.copy_from(csv_io, 'statspopulation', sep=delimiter)
        connection.commit()
        cursor.close()
    except Exception as e:
        connection.rollback()
        raise e
    

def split_grage(code):
    """
    Genere le label du groupe d'âges à partir du code
    Préecondition: code est de la forme "AGE_AGE", ex: "15_18", "MN_20", "14_PL"
    """
    age_range = code.split('_')
    
    if age_range[0] == 'MN':
        return "Moins de " + age_range[1]+ " ans"
    else:
        if age_range[1] == "PL":
            return  age_range[0] + " ans ou plus"
        else:
            return "De " + age_range[0] + " à " + age_range[1]+ " ans"


def read_dep_one_at_six(stats_buffer, data_buffer, last_stat_id, dep, annee, first = 0, last = None, delimiter=";"):
    """
    Permet de lire et de reccuperer le contenu d'un fichier DEP, actuellement il fonctionne avec tout les fichiers DEP1 à DEP.
    first et last indique la premiere et la derniere ligne à importer [first, last],
    """
    liste_stats_buffer =[]
    liste_data_buffer = []
    

    df_dep = pd.read_csv(dep["fichier"], delimiter=delimiter)
    title = dep["titre"] 
    
    df_dep.rename(columns=lambda x: 'TYPMAR' if x.startswith('TYPMAR') else 
                                  'id_dep' if x.startswith('REGDEP') else 
                                  'NBVALUE' if x.startswith('NBMAR') else 
                                  'NATEPOUX' if x.endswith('EPOUX') else x,
              inplace=True)
    
    for index, row in df_dep.iterrows():
        # Si la borne inferrieur est atteinte, on commence la copie
        if index >= first :  
            id_dep = row['id_dep']
            # Vérification de la longueur de id_dep et des deux derniers caractères : pas besoin des totals de regions ou de categorie, ca ne serait que redondons en base de données
            if (len(id_dep) != 4 and len(id_dep) !=3 ) or (id_dep[-2:].lower() == "xx"):
                continue  # Passer au tour de boucle suivant
            else: 
                id_dep = id_dep[-2:]
            last_stat_id += 1
            liste_stats_buffer.append({'id': last_stat_id, 'titre': title})
            
            for column in df_dep.columns:
                if column != 'id_dep':
                    liste_data_buffer.append({'id_dep': id_dep, 
                                    'value': row[column], 
                                    'annee': annee, 
                                    'code_categorie': column, 
                                    'id_statistique_mariage': last_stat_id})
            # Si la borne superrieur est atteinte, on s'arrete
            if last != None and index >= last:
                break

    stats_buffer = pd.concat([stats_buffer, pd.DataFrame(liste_stats_buffer)], ignore_index=True)
    data_buffer = pd.concat([data_buffer, pd.DataFrame(liste_data_buffer)], ignore_index=True)
    
    return stats_buffer, data_buffer, last_stat_id





def insert_dep_one_at_six(connection, csv_mariage, annee, first = 0, last = None, delimiter=";"):
    """
    Permet d'inseret toutes les données contenu dans DEP1 à DEP6. 
    first et last indique la premiere et la derniere ligne à importer [first, last],
    """
    cursor = connection.cursor() 

    stats_buffer = pd.DataFrame(columns=['id', 'titre'])
    data_buffer = pd.DataFrame(columns=['id_dep', 'value', 'annee', 'code_categorie', 'id_statistique_mariage'])
    last_stat_id = get_last_stat_id(connection)
    for key, datas in csv_mariage.items():  
        stats_buffer, data_buffer, last_stat_id = read_dep_one_at_six(stats_buffer, data_buffer,last_stat_id, datas, annee, first, last, delimiter) 
    
    try: 
        # Insertion dans StatistiqueMariage
        # Convertit le DataFrame en chaîne CSV
        csv_data = stats_buffer.to_csv(index=False, sep=delimiter, header=False)

        # Crée un flux de texte en mémoire
        csv_io = io.StringIO()
        csv_io.write(csv_data)
        csv_io.seek(0)

        # Insere les données
        cursor.copy_from(csv_io, 'statistiquemariage', sep=delimiter) 
        
        # Insertion dans DatasStatsMariage
        # Convertit le DataFrame en chaîne CSV
        csv_data = data_buffer.to_csv(index=False, sep=delimiter, header=False)

        # Crée un flux de texte en mémoire
        csv_io = io.StringIO()
        csv_io.write(csv_data)
        csv_io.seek(0)

        # Insere les données
        cursor.copy_from(csv_io, 'datasstatsmariage', sep=delimiter) 

        connection.commit()
        cursor.close()
    except Exception as e:
        connection.rollback()
        raise e


def initialise_db(connection, params, flag_reset_bdd=False):
    """
        Initialise la base de donnée avec les données minimales à avoir (Region, departement, commune)
    """
    if flag_reset_bdd:
        reset_bdd(connection, params, force="RESET")
    chef_lieu_reg =  insert_regions(connection, params["cog"]["v_region_2023"])
    chef_lieu_dep = insert_departements(connection, params["cog"]["v_departement_2023"]) 
    insert_communes(connection, params["cog"]["v_commune_2023"])
    insert_cheflieu(connection, chef_lieu_reg, chef_lieu_dep)

def insert_stats_population_fragment(connection, params, annee, init_metadonnee =False, first = 0, last = None, verbose = False):
    """
        Insert les statistiques sur la population, 
        first et last indique la premiere et la derniere ligne à importer [first, last],
        si last est à None toutes les statistiques sur la population seronts importer.
        init_metadonnee permet d'indiquer si on doit initialiser la table des StatsLibelle
    """
    if verbose :
        message = "Insertion des statistiques de population à partir de "+ str(first) 
        if last : 
            message= message + " à " + str(last)+ "."
        else:
            message += " jusqu'à la fin du fichier."
        print(message)
    if init_metadonnee == True:
        insert_stats_libelle(connection, params["csv_historique"]["meta_cc_serie_historique_2020"])
    insert_stats_population(connection, params["csv_historique"]["base_cc_serie_historique_2020"], annee, first, last)

def insert_dep_file_fragment(connection, params, annee, first = 0, last= None, verbose= False):
    """
        Insert les statistiques sur les mariages (fichier DEP1 à DEP6), 
        first et last indique la premiere et la derniere ligne à importer [first, last] pour chaque fichier,
        si last est à None toutes les statistiques sur les mariages seronts importer
    """
    if verbose :
        message = "Insertion des statistiques de mariage à partir de "+ str(first) 
        if last : 
            message= message + " à " + str(last)+"."
        else:
            message += " jusqu'à la fin des fichiers."
        print(message)
    insert_dep_one_at_six(connection, params["csv_mariage"], annee, first, last)


def format_tuples(tuples):
    """
    Format une liste de tuples pour un affichage au terminal
    """ 
    return ', '.join(name[1] for name in tuples)