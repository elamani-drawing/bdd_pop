-- Question 3
-- Cette procedure ne peut être executé qu'apres avoir executé alter.sql pour ajouter les attributs population dans les tables regions et departement
CREATE OR REPLACE PROCEDURE Update_Population_Departments_And_Regions()
LANGUAGE plpgsql
AS $$
BEGIN
    -- Met à jour la population des départements
    UPDATE Departement dp
    SET population = subquery.total_population
    FROM (
        SELECT cm.id_dep, SUM(sp.valeur) AS total_population
        FROM Commune cm
        JOIN StatsPopulation sp ON cm.com = sp.id_com
        WHERE sp.code_stats_libelle = 'P20_POP'
        GROUP BY cm.id_dep
    ) AS subquery
    WHERE dp.dep = subquery.id_dep;

    -- Met à jour la population des régions
    UPDATE Region rg
    SET population = subquery.total_population
    FROM (
        SELECT dp.id_reg, SUM(dp.population) AS total_population
        FROM Departement dp
        GROUP BY dp.id_reg
    ) AS subquery
    WHERE rg.reg = subquery.id_reg;
END;
$$;



-- Question 3 suite
-- Création d'un trigger AFTER UPDATE sur sur la table StatsPopulation
CREATE OR REPLACE FUNCTION update_departments_and_regions_population()
RETURNS TRIGGER AS $$
BEGIN
    -- Appel la procédure pour mettre à jour les populations des départements et des régions
    PERFORM Update_Population_Departments_And_Regions();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER after_update_statspopulation
AFTER UPDATE ON StatsPopulation
FOR EACH ROW
EXECUTE FUNCTION update_departments_and_regions_population();


