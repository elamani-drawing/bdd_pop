params = {
    "sql_requete" : {  
        "create_sql" : "./sql/create.sql", 
        "delete_sql" : "./sql/delete.sql",
        "view_sql" : "./sql/view.sql",
        "alter_sql" : "./sql/alter.sql",
        "procedure_sql" : "./sql/procedure.sql",
        "trigger_sql" : "./sql/trigger.sql",
    },
    "csv_historique" : {
        "meta_cc_serie_historique_2020" : "./csv/historique_2020/meta_base-cc-serie-historique-2020.CSV",
        "base_cc_serie_historique_2020" : "./csv/historique_2020/base-cc-serie-historique-2020.CSV",
    },
    "csv_mariage" : {
        "dep1": {
            "fichier" : "./csv/mariage/Dep1.csv",
            "titre" : "Groupe d'âges des époux selon le département et la région de mariage. Année 2021",
        },
        "dep2": {
            "fichier" : "./csv/mariage/Dep2.csv",
            "titre" : "État matrimonial antérieur des époux selon le département et la région de mariage. Année 2021",
        },
        "dep3": {
            "fichier" : "./csv/mariage/Dep3.csv",
            "titre" : "Groupe d'âges des époux se mariant pour la première fois selon le département et la région de mariage. Année 2021",
        },
        "dep4": {
            "fichier" : "./csv/mariage/Dep4.csv",
            "titre" : "Nationalité des époux selon le département et la région de domicile conjugal. Année 2021",
        },
        "dep5": {
            "fichier" : "./csv/mariage/Dep5.csv",
            "titre" : "Pays de naissance des époux selon le département et la région de domicile conjugal. Année 2021",
        },
        "dep6": {
            "fichier" : "./csv/mariage/Dep6.csv",
            "titre" : "Répartition mensuelle des mariages selon le département et la région de mariage. Année 2021",
        },
    },
    "cog" : {
        "v_commune_2023" : "./csv/cog/v_commune_2023.csv",
        "v_departement_2023" : "./csv/cog/v_departement_2023.csv",
        "v_region_2023" : "./csv/cog/v_region_2023.csv",
    }
}

# Les types sont principalement utilisées pour le formmatage lors de l'affichage

TYPE_MAR_LABEL= {
    "HF": "Mariages entre personnes de sexe différent",
    "HH-FF": "Mariages entre personnes de même sexe",
    "HH": "Mariages entre hommes",
    "FF": "Mariages entre femmes",
    "HF-H": "Mariages entre personnes de sexe différent - Hommes",
    "HF-F": "Mariages entre personnes de sexe différent - Femmes",
}

TYPE_ETAT_MAT_LABEL = {
    'E': 'Ensemble',
    '1': 'Célibataires',
    '3': 'Veufs / Veuves',
    '4': 'Divorcés / Divorcées',
}

TYPE_SEXE = {
    "H": "Hommes",
    "F": "Femmes" ,
}

TYPE_DE_CATEGORIE = {
    "TYPMAR" : "Type de mariage",
    "SEXE": "Sexe",
    "ETAMAT" : "État matrimonial",
}

TYPE_NAT_EPOUX =  {
    "TOTAL": "Ensemble",
    "FR_FR": "Les deux époux(ses) français(es)",
    "FR_ETR": "Couples mixtes",
    "ETR_ETR": "Les deux époux(ses) étrangers(ères)"
}

TYPE_MMAR = {
    "AN": "Année",
    "01": "Janvier",
    "02": "Février",
    "03": "Mars",
    "04": "Avril",
    "05": "Mai",
    "06": "Juin",
    "07": "Juillet",
    "08": "Août",
    "09": "Septembre",
    "10": "Octobre",
    "11": "Novembre",
    "12": "Décembre"
}

CATEGORIE_STATS = {
    "TYPMAR" : TYPE_MAR_LABEL, 
    "ETATMAT" : TYPE_ETAT_MAT_LABEL,
    "SEXE" : TYPE_SEXE,
    "NATEPOUX" : TYPE_NAT_EPOUX,
    "MMAR" : TYPE_MMAR,
}
