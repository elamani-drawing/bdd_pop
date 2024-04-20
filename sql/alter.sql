-- Question 3
-- Ajoute un nouveau champ pour stocker la population totale dans la table Departement
ALTER TABLE Departement ADD COLUMN population DECIMAL(20, 10);

-- Ajoute un nouveau champ pour stocker la population totale dans la table Region
ALTER TABLE Region ADD COLUMN population DECIMAL(20, 10);
