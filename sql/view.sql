-- Question 2
-- La population totale de chaque département pour chaque année disponible dans la table StatsPopulation

CREATE OR REPLACE VIEW Vue_Population_Departements AS
SELECT dp.dep AS id_dep,
       dp.ncc,
       sp.annee ,
       SUM(sp.valeur) AS population
FROM Departement dp
JOIN Commune cm ON dp.dep = cm.id_dep
JOIN StatsPopulation sp ON cm.com = sp.id_com
WHERE sp.code_stats_libelle = 'P20_POP' 
GROUP BY dp.dep, dp.ncc, sp.annee;

-- La population totale de chaque région pour chaque année disponible dans la table StatsPopulation.
CREATE OR REPLACE VIEW Vue_Population_Regions AS
SELECT rg.reg AS id_reg,
       rg.ncc ,
       sp.annee ,
       SUM(sp.valeur) AS population
FROM Region rg
JOIN Departement dp ON rg.reg = dp.id_reg
JOIN Commune cm ON dp.dep = cm.id_dep
JOIN StatsPopulation sp ON cm.com = sp.id_com
WHERE sp.code_stats_libelle = 'P20_POP' 
GROUP BY rg.reg, rg.ncc, sp.annee;
