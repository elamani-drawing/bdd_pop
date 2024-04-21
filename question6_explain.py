from lib import initialise_db, get_connection, insert_stats_population_fragment, insert_dep_file_fragment, execute_view_bdd, execute_alter_procedure, execute_sql_file
from config import params
from requete import explain_get_departments_in_region, explain_get_communes_with_population_greater_than

# Configurer la connection dans config.py
connection = get_connection() 

def restore_data(connection, params):
    initialise_db(connection, params, flag_reset_bdd=True)
    insert_stats_population_fragment(connection, params, annee="2020", init_metadonnee= True, verbose=True)
    insert_dep_file_fragment(connection, params,"2021", verbose=True)
    execute_view_bdd(connection, params) # genere les view du fichier view.sql
    execute_alter_procedure(connection, params) # execute le fichier alter.sql et procedure.sql, comme indiquer a la question 3
    execute_sql_file(connection, params["sql_requete"]["trigger_sql"]) 
    
        
# Vous pouvez commenter cette ligne apres qu'elle est etait executé au moin une fois 
restore_data(connection, params) 

res = explain_get_departments_in_region(connection, 11)
print("""
    Pour la première requête, le plan d'exécution dépendra de l'indexation et de la cardinalité de la table Departement ainsi que de la sélectivité 
    de la clause WHERE. Si l'index sur id_reg est efficace et la cardinalité des départements dans une région est relativement faible, une 
    simple lecture séquentielle ou un balayage de l'index pourrait être utilisé. Cela devrait être relativement rapide.
""")
print("Résultat:" + str(res))
print("""
    "Seq Scan (Balayage séquentiel)", cela signifie que PostgreSQL a effectué un balayage séquentiel de la table Departement. 
    Il a parcouru toute la table de manière séquentielle pour trouver les lignes qui correspondent au critère de filtrage.
    Cost (Coût) : Le coût estimé de cette opération est de 0.00 à 2.26 unités.
    Filter (Filtre) : Il applique ensuite un filtre pour ne renvoyer que les lignes où id_reg est égal à 11, comme cela a été spécifier dans la requête.\n
""")

res = explain_get_communes_with_population_greater_than(connection, "34", "10000")
print("""
    Pour la deuxième requête, le plan d'exécution dépendra également de l'indexation et de la cardinalité des tables Commune et StatsPopulation, ainsi que de la 
    sélectivité des conditions/strategie de jointure et de filtrage.
""")
print("Résultat:" + str(res))
print("""
    Nested Loop Join (Jointure en boucle imbriquée) : Nous pouvons voir que la requête utilise une jointure en boucle imbriquée pour combiner les résultats des tables Commune et StatsPopulation.
    Seq Scan (Balayage séquentiel) : PostgreSQL a effectué un balayage séquentiel de la table Commune pour trouver les lignes correspondant au filtre id_dep = '34'.
    Index Scan (Balayage indexé) : Il a également utilisé un balayage indexé sur la table StatsPopulation en utilisant la clé primaire pour trouver les lignes correspondant aux conditions id_com = comm.com et code_stats_libelle = 'P20_POP'.
    Sort (Tri) : Avant de renvoyer les résultats, les données sont triées par comm.com et comm.ncc.
    Unique (Unique) : Pour finirr, une opération d'élimination des doublons est effectuée pour ne renvoyer que des lignes uniques.
""")

connection.close()