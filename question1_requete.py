from lib import initialise_db, get_connection, insert_stats_population_fragment, insert_dep_file_fragment, format_tuples
from config import params
from requete import get_departments_in_region, get_communes_with_population_greater_than, get_most_populated_commune, get_least_populated_commune, get_most_populated_region, get_least_populated_region


# Configurer la connection dans config.py
connection = get_connection() 


def restore_data(connection, params):
    initialise_db(connection, params, flag_reset_bdd=True)
    insert_stats_population_fragment(connection, params, annee="2020",init_metadonnee= True, verbose=True)  
    insert_dep_file_fragment(connection, params,"2021", verbose=True)  

def print_dep_in_reg(connection, region_id): 
    departments = get_departments_in_region(connection, region_id) 
    print("\n- Les departements de la région {} sont: {}".format(region_id,format_tuples(departments)))

def print_com_greather_than(connection, dep_id, than):
    communes = get_communes_with_population_greater_than(connection, dep_id, than)
    print("\n- Les communes avec plus de {} personnes dans le departement {} sont: {}".format(than, dep_id, format_tuples(communes)))

def print_most_populated_com(connection, id_dep):
    commune = get_most_populated_commune(connection.cursor(), id_dep)
    print("\n- La commune la plus peuplé du département {} est: {} avec la {} habitants".format(id_dep, commune[1], commune[2]))

def print_least_populated_com(connection, id_dep):
    commune = get_least_populated_commune(connection.cursor(), id_dep)
    print("\n- La commune la moin peuplé du département {} est la {} avec {} habitants".format(id_dep, commune[1], commune[2]))

def print_most_populated_region(connection):
    region = get_most_populated_region(connection.cursor())
    print("\n- La région la plus peuplé est la {} avec {} habitants".format(region[1], region[2]))
    
def print_least_populated_region(connection):
    region = get_least_populated_region(connection.cursor())
    print("\n- La région la moin peuplé est la {} avec {} habitants".format(region[1], region[2]))

# Vous pouvez commenter cette ligne apres qu'elle est etait executé au moin une fois 
restore_data(connection, params) 

print_dep_in_reg(connection, region_id=11) # Affiche les departements de la region 11
print_com_greather_than(connection, dep_id=34, than = 10000) # Affiche les communes de plus de 1000 personnes dans le departement demander
print_most_populated_com(connection, 34) # Affiche la commune la plus peuplé du departement 34
print_least_populated_com(connection, 34) # la moin peuplé
print_most_populated_region(connection) # Affiche la région la plus peuplé
print_least_populated_region(connection) # la moin peuplé

connection.close()