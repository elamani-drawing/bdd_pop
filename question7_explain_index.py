from lib import initialise_db, get_connection, insert_stats_population_fragment, insert_dep_file_fragment, execute_view_bdd, execute_alter_procedure, execute_sql_file
from config import params
from requete import explain_primary_key_index_departement, explain_communes_with_less_than_population

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

res = explain_primary_key_index_departement(connection, '01')
print("Résultat de la premiere requete:" + str(res))
print("""
    Index Scan (Balayage indexé) : PostgreSQL utilise un balayage indexé sur la clé primaire departement_pkey de la table Departement. Cela signifie qu'il 
    accède directement à la ligne correspondant à la clé primaire '01' en utilisant l'index, ce qui est efficace en termes de performances.
    Cost (Coût) : Le coût estimé de cette opération est de 0.15 à 8.17 unités. \n
""")

res = explain_communes_with_less_than_population(connection, "5000")

print("Résultat de la deuxieme requete:" + str(res))
print("""
    Nested Loop Join (Jointure en boucle imbriquée) : PostgreSQL utilise une jointure en boucle imbriquée pour combiner les résultats 
     des tables Commune et StatsPopulation.
    Seq Scan (Balayage séquentiel) : Il effectue d'abord un balayage séquentiel de la table Commune pour obtenir toutes les communes.
    Index Scan (Balayage indexé) : Ensuite, il utilise un balayage indexé sur la table StatsPopulation en utilisant la clé primaire 
     pour trouver les lignes correspondant aux conditions id_com = comm.com et code_stats_libelle = 'P20_POP'.
    Coût : Le coût total estimé de cette opération est de 0.42 à 15439.03 unités.
""")
print("""
- Analyse des 2 premieres requetes:
    Dans la première requête, PostgreSQL utilise efficacement l'index sur la clé primaire pour accéder rapidement à la ligne spécifique 
    correspondant à la valeur '01' dans la table Departement. Cela se traduit par un coût d'exécution relativement faible.
    En revanche, dans la deuxième requête, bien qu'un index soit utilisé pour accéder aux données de la table StatsPopulation, la performance 
    est affectée par la nécessité d'effectuer une jointure en boucle imbriquée avec la table Commune et une opération de filtrage. 
    Le coût d'exécution est donc plus élevé, ce qui pourrait indiquer une moins bonne performance par rapport à une requête 
    qui pourrait tirer parti d'autres stratégies de jointure ou d'indexation.
""")

res = explain_communes_with_less_than_population(connection, "5000", make_index=True)
print("Résultat de la troisieme requete:" + str(res))

print("""
Malgré l'ajout de l'index sur l'attribut valeur (population), PostgreSQL a choisi une stratégie de balayage séquentiel (Seq Scan) pour accéder aux données 
    de la table StatsPopulation. Nous pouvons voir que PostgreSQL à paralléliser de la requête, cela peut aider à améliorer les performances en exploitant les 
    capacités multicœurs du système.

Nous pouvons confirmer que PostgreSQL effectue effectivement les sélections individuelles avant de calculer les jointures. Il utilise une jointure de hachage (Hash Join) entre les tables Commune et StatsPopulation, en examinant cet opérateur de jointure, nous pouvons voir qu'il a deux entrées : la première est le résultat d'un balayage séquentiel parallèle (Parallel Seq Scan) de la table StatsPopulation, et la seconde est le résultat d'un balayage séquentiel (Seq Scan) de la table Commune. 

En conclusion les résultats des deux dernieres requêtes EXPLAIN avec ou sans index sur valeur (population) sont identiques. Cela suggère que l'ajout de l'index sur la colonne population dans la table StatsPopulation n'a pas eu d'impact significatif sur le plan d'exécution de la requête.
PostgreSQL n'a pas choisi d'utiliser cet index pour accéder aux données, préférant plutôt un balayage séquentiel parallèle.
""")

connection.close()