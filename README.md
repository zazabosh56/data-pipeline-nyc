# 1. Cloner le projet et s’y placer
Ouvre un terminal, puis :

git clone <url_du_repo>
cd <chemin_du_projet>

# 2. Installation des dépendances pour développement
source venv/bin/activate      # Sur Linux/Mac
venv\Scripts\activate         # Sur Windows

pip install -r requirements.txt

3. Lancement de l’architecture Docker
Dans le terminal, assure-toi d’être dans le dossier racine du projet (là où il y a le docker-compose.yml), puis exécute :
# (1) Construire les images si besoin (optionnel, seulement si tu modifies le Dockerfile)
docker-compose build

# (2) Migrer la base de données Airflow si première utilisation (nécessaire une seule fois)
docker-compose run --rm airflow-webserver airflow db migrate

# (3) Créer un utilisateur admin pour Airflow (nécessaire seulement la première fois)
docker-compose run --rm airflow-webserver airflow users create \
    --username admin --password admin --firstname Admin --lastname User \
    --role Admin --email admin@example.com

# (4) Lancer tous les services en arrière-plan (mode détaché)
docker-compose up --build -d

4. Accès à Airflow
Airflow Web UI : http://localhost:8080
Identifiant : admin
Mot de passe : admin

6. Arrêter les services
docker-compose down
