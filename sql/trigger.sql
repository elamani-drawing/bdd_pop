-- question 4: Bloque les INSERT, UPDATE, DELETE sur les tables Departement, region

-- Trigger pour bloquer les INSERT sur la table Region
CREATE OR REPLACE FUNCTION prevent_region_modification()
RETURNS TRIGGER AS $$
BEGIN
    RAISE EXCEPTION 'La table Region ne peut pas être modifiée';
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER before_insert_region
BEFORE INSERT ON Region
FOR EACH ROW
EXECUTE FUNCTION prevent_region_modification();

-- Trigger pour bloquer les UPDATE sur la table Region
CREATE TRIGGER before_update_region
BEFORE UPDATE ON Region
FOR EACH ROW
EXECUTE FUNCTION prevent_region_modification();

-- Trigger pour bloquer les DELETE sur la table Region
CREATE TRIGGER before_delete_region
BEFORE DELETE ON Region
FOR EACH ROW
EXECUTE FUNCTION prevent_region_modification();

-- Trigger pour bloquer les INSERT sur la table Departement
CREATE OR REPLACE FUNCTION prevent_department_modification()
RETURNS TRIGGER AS $$
BEGIN
    RAISE EXCEPTION 'La table Departement ne peut pas être modifiée';
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER before_insert_department
BEFORE INSERT ON Departement
FOR EACH ROW
EXECUTE FUNCTION prevent_department_modification();

-- Trigger pour bloquer les UPDATE sur la table Departement
CREATE TRIGGER before_update_department
BEFORE UPDATE ON Departement
FOR EACH ROW
EXECUTE FUNCTION prevent_department_modification();

-- Trigger pour bloquer les DELETE sur la table Departement
CREATE TRIGGER before_delete_department
BEFORE DELETE ON Departement
FOR EACH ROW
EXECUTE FUNCTION prevent_department_modification();

-- Pour le trigger qui met à jour la population dans departement et region, voir procedure.sql : Question 3 suite



-- Question 5: Trigger suite
CREATE OR REPLACE FUNCTION update_population_on_new_year()
RETURNS TRIGGER AS $$
DECLARE
    stats_label_code VARCHAR(20);
    total_communes_per_department INTEGER;
BEGIN
    -- Récupère le code de l'étiquette statistique de l'élément inséré
    stats_label_code := NEW.code_stats_libelle;

    -- Vérifie si le code statistique concerne la population
    IF stats_label_code LIKE '%POP%' THEN
        -- Vérifie que toutes les communes de département ont des données pour la nouvelle année
        SELECT COUNT(*) INTO total_communes_per_department
        FROM Departement dp
        JOIN Commune cm ON dp.dep = cm.id_dep
        LEFT JOIN StatsPopulation sp ON cm.com = sp.id_com AND sp.code_stats_libelle = stats_label_code AND sp.annee = NEW.annee
        GROUP BY dp.dep
        HAVING COUNT(sp.id_com) = COUNT(cm.com);
        
        -- Met à jour la population des départements si toutes les communes de département ont des données pour la nouvelle année
        -- Si toutes les les communes de département ont des données pour la nouvelle année cela implique que toutes les communes de régions également
        IF total_communes_per_department = (SELECT COUNT(*) FROM Departement) THEN
            -- Met à jour la population des régions et departement
            PERFORM Update_Population_Departments_And_Regions();
        END IF;
    END IF;
    
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Création du déclencheur pour mettre à jour la population lors de l'ajout de nouvelles données de recensement
CREATE TRIGGER after_insert_statspopulation
AFTER INSERT ON StatsPopulation
FOR EACH ROW
EXECUTE FUNCTION update_population_on_new_year();