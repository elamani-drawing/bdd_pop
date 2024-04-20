import psycopg2
from psycopg2 import sql

def get_last_stat_id(connection):
    """
    Reccupere le dernier id (le plus élevé) dans la table StatistiqueMariage.
    Renvoie 0 si la table est vide
    """
    try:
        cursor = connection.cursor()
        cursor.execute("SELECT MAX(id) FROM StatistiqueMariage;")
        last_stat_id = cursor.fetchone()[0]
        cursor.close()
        if last_stat_id : return last_stat_id 
        else: return 0
    except Exception as e:
        raise e


def get_departments_in_region(connection, region_id):
    """
    Reccupere la liste des departement d'une region
    """
    cursor = connection.cursor()
    cursor.execute("SELECT dep, ncc FROM Departement WHERE id_reg = %s", (region_id,))
    departments = cursor.fetchall()
    cursor.close()
    return departments


def get_communes_with_population_greater_than(connection, department_id, greater_than, code_stats_libelle ="P20_POP"):
    """
    Récupére la liste des communes avec une population supérieure à X dans un département donné
    """
    cursor = connection.cursor()
    cursor.execute(" SELECT DISTINCT(comm.com), comm.ncc FROM Commune comm, StatsPopulation sp  WHERE sp.code_stats_libelle = %s AND sp.id_com = comm.com AND comm.id_dep = '%s' AND sp.valeur > %s", (code_stats_libelle, department_id, greater_than))
    communes = cursor.fetchall()
    cursor.close()
    return communes


def get_most_populated_commune(cursor, id_dep, code_stats_libelle="P20_POP"):
    """
    Récupérer la commune la plus peuplée d'un département
    """
    query = """
        SELECT com, ncc AS commune_name, valeur AS population
        FROM Commune
        JOIN StatsPopulation ON Commune.com = StatsPopulation.id_com
        WHERE StatsPopulation.code_stats_libelle = %s AND id_dep = '%s' 
        ORDER BY valeur DESC
        LIMIT 1;
    """
    cursor.execute(query, (code_stats_libelle, id_dep,))
    commune =  cursor.fetchone()
    cursor.close()
    return commune


def get_least_populated_commune(cursor, id_dep, code_stats_libelle="P20_POP"):
    """
    Récupérer la commune la moins peuplée d'un département
    """
    query = """
        SELECT com, ncc AS commune_name, valeur AS population
        FROM Commune
        JOIN StatsPopulation ON Commune.com = StatsPopulation.id_com
        WHERE StatsPopulation.code_stats_libelle = %s AND id_dep = '%s'
        ORDER BY valeur
        LIMIT 1;
    """
    cursor.execute(query, (code_stats_libelle, id_dep,))
    commune = cursor.fetchone()
    cursor.close()
    return commune


def get_most_populated_region(cursor, code_stats_libelle="P20_POP"):
    """
    Récupére la région la plus peuplée
    """
    query = """
        SELECT reg, Region.ncc AS region_name, SUM(valeur) AS total_population
        FROM Commune
        JOIN StatsPopulation ON Commune.com = StatsPopulation.id_com
        JOIN Departement ON Commune.id_dep = Departement.dep
        JOIN Region ON Departement.id_reg = Region.reg
        WHERE StatsPopulation.code_stats_libelle = %s 
        GROUP BY reg, region_name
        ORDER BY total_population DESC
        LIMIT 1;
    """
    cursor.execute(query, (code_stats_libelle,))
    region= cursor.fetchone()
    cursor.close()
    return region

def get_least_populated_region(cursor, code_stats_libelle="P20_POP"):
    """
    Récupére la région la moins peuplée
    """
    query = """
        SELECT reg, Region.ncc AS region_name, SUM(valeur) AS total_population
        FROM Commune
        JOIN StatsPopulation ON Commune.com = StatsPopulation.id_com
        JOIN Departement ON Commune.id_dep = Departement.dep
        JOIN Region ON Departement.id_reg = Region.reg
        WHERE StatsPopulation.code_stats_libelle = %s 
        GROUP BY reg, region_name
        ORDER BY total_population
        LIMIT 1;
    """
    cursor.execute(query,(code_stats_libelle,))
    region = cursor.fetchone()
    cursor.close()
    return region

def view_get_population_departments(connection):
    """
    Affiche la population des départements par année.
    """ 
    cursor = connection.cursor()
    cursor.execute("SELECT * FROM Vue_Population_Departements;")
    departments_population = cursor.fetchall()
    cursor.close()
    return departments_population

def view_get_population_regions(connection):
    """
    Affiche la population des régions par année.
    """ 
    cursor = connection.cursor()
    cursor.execute("SELECT * FROM Vue_Population_Regions;")
    regions_population = cursor.fetchall()
    cursor.close()
    return regions_population

def get_population_departments_after_alter(connection):
    """
    Récupère la population des départements après l'altération des tables.
    """
    cursor = connection.cursor()
    cursor.execute("SELECT dep, ncc, population FROM Departement;")
    departments_population = cursor.fetchall()
    cursor.close()
    return departments_population

def get_population_regions_after_alter(connection):
    """
    Récupère la population des régions après l'altération des tables.
    """
    cursor = connection.cursor()
    cursor.execute("SELECT reg, ncc, population FROM Region;")
    regions_population = cursor.fetchall()
    cursor.close()
    return regions_population



def insert_region(connection, reg, ncc):
    """
    Insere une nouvelle region dans la BDD
    """
    try:
        cursor = connection.cursor()
        insert_query = sql.SQL("INSERT INTO Region (reg, ncc) VALUES ({}, {})").format(
            sql.Literal(reg), sql.Literal(ncc)
        )
        cursor.execute(insert_query)
        connection.commit()
        print("Données insérées avec succès dans la table Region!")
    except (Exception, psycopg2.DatabaseError) as error:
        print("Erreur lors de l'insertion des données dans la table Region: ", error)
    finally:
        if cursor:
            cursor.close()