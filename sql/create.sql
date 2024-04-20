CREATE TABLE Region (
    reg INT PRIMARY KEY,
    ncc VARCHAR(100),
    CONSTRAINT check_reg_length CHECK (reg >= 1 AND reg <= 999)
);
 
CREATE TABLE Departement (
    dep VARCHAR(3) PRIMARY KEY,
    ncc VARCHAR(100),
    id_reg INT,
    FOREIGN KEY (id_reg) REFERENCES Region(reg)
);

 
CREATE TABLE Commune (
    com VARCHAR(5) PRIMARY KEY,
    ncc VARCHAR(100),
    id_dep VARCHAR(3),
    FOREIGN KEY (id_dep) REFERENCES Departement(dep)
);


CREATE TABLE ChefLieuDep (
    id_dep VARCHAR(3) PRIMARY KEY,
    id_com VARCHAR(5),
    FOREIGN KEY (id_dep) REFERENCES Departement(dep),
    FOREIGN KEY (id_com) REFERENCES Commune(com)
);


CREATE TABLE ChefLieuReg (
    id_reg INT PRIMARY KEY,
    id_com VARCHAR(5),
    FOREIGN KEY (id_reg) REFERENCES Region(reg),
    FOREIGN KEY (id_com) REFERENCES Commune(com)
);

CREATE TABLE StatsLibelle (
    code VARCHAR(20) PRIMARY KEY,
    libelle VARCHAR(100) 
);
 
CREATE TABLE StatsPopulation (
    id_com VARCHAR(5),
    code_stats_libelle VARCHAR(20),
    annee INT CHECK (annee >= 1900 AND annee <= 3000),
    valeur DECIMAL(20, 10), -- Le premier nombre indique la précision totale, le second la précision après la virgule
    FOREIGN KEY (id_com) REFERENCES Commune(com),
    FOREIGN KEY (code_stats_libelle) REFERENCES StatsLibelle(code),
    PRIMARY KEY (id_com, code_stats_libelle)
);


CREATE TABLE StatistiqueMariage (
    id INT PRIMARY KEY,
    titre VARCHAR
);

CREATE TABLE DatasStatsMariage (
    id_dep VARCHAR(3),
    value VARCHAR,
    annee INT CHECK (annee > 1900 AND annee < 3000),
    code_categorie VARCHAR(10),
    id_statistique_mariage INT,
    FOREIGN KEY (id_statistique_mariage) REFERENCES StatistiqueMariage(id),
    PRIMARY KEY (id_statistique_mariage, id_dep, annee, code_categorie)
);