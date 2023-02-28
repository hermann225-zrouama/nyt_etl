# new_york_times_etl

Un ETL (Extract - Transform - Load ) est un processus qui permet de collecter les données de plusieurs sources, d’y effectuer des transformations et de les charger dans un data warehouse, une base de données, un datalake ou autre… L’intérêt de la mise en place de tel système est donc de centraliser et consolider les données un seul référentiel fiable utile pour des analyses permettant de faciliter la prise de décisions. Ce projet à pour objectif la construction d'un ETL qui permettrait de collecter, traiter et stocker les articles issue de la plateforme du média américain *The New York Times*.

# Arborescence du projet

- ├── Dockerfile
- ├── README.md
- ├── airflow.cfg
- ├── dags
- │   ├── dag.py
- │   └── scripts
- ├── data
- │   ├── author_data.csv
- │   ├── keyword_data.csv
- │   ├── nyt_economy.parquet
- │   └── nyt_economy_brute.csv
- ├── docker-compose.yml
- ├── entrypoint.sh
- ├── requirements.txt
- └── webserver_config.py

- airflow.cfg: fichier de configuration de airflow
- dags: dossier contenant les DAGs
- scripts: dossier contenant les scripts python
- data: dossier contenant les données
- docker-compose.yml: fichier de configuration de docker-compose
- entrypoint.sh: fichier d'entrée de l'application
- notebooks: dossier contenant les notebooks de test
- requirements.txt: fichier contenant les dépendances du projet
- webserver_config.py: fichier de configuration de l'interface web de airflow

# Comment Run de l'application ?

* 1- pour ce faire veuillez exécuter au dossier racine du projet la commande *docker build --rm -t docker_airflow:latest .* 
* 2- puis exécuter la commande *docker-compose up -d* pour lancer l'application
* 3- Veuillez sur l'interface de docker afin de verifier que les containers sont bien lancés. Par mésure de précaution, veuillez attendre au moins **3** à **5** minutes avant de continuer.
* 4- Veuillez ensuite vous rendre sur l'interface de airflow à l'adresse *http://localhost:8080/* afin de lancer l'ETL.
* 5- rechercher le DAG *new_financial_etl* et cliquer sur le bouton afin de lancer l'ETL.

# Bonus : Comment visualiser les données ?
* 1- Veuillez vous rendre à l'adresse *http://localhost:3001 afin d'acceder à l'interface de *metabase*.
* 2- Cliquer sur **lets get started** afin de créer un compte.
* 3- Choisissez la langue de votre choix.
* 4- remplissez les chmaps avec vos informations puis cliquer sur **suivant**.
* 5- Veuillez choisir la base de données *postgres* puis entrez les informations suivantes:
    - Afficher le nom de la base de données: *newyorktimes*
    - Host: *postgres*
    - Port: *5432*
    - database name: *airflow*
    - username: *airflow*
    - password: *airflow*
* 6- Laissez les autres champs par défaut puis cliquer sur **connecter à la base de données** puis sur **terminer**.
* 7- Veuillez cliquer sur **Emmenez-moi dans metabase**.

Nos données sont stockées dans la table *articles*.
- Une fois là vous pouvez avoir des informations globales sur les données. en cliquant sur **Un aperçu de articles**
- Pour visualiser nos données cliquez sur **parcourir les données* dans l'onglet gauche
- Là cliquer sur notre base de données *newyorktimes* puis sur la table *Articles* là on a un aperçu tabulaire de nos données.

# Explication des données
On a trois principaux fichiers de données:
- **author_data.csv**: contient les informations sur les auteurs des articles
- **keyword_data.csv**: contient les mots clés associés aux articles
- **nyt_economy.parquet**: contient les articles stockés en fonction de la date (année, mois et jour)

# Quelques visuels

Vous trouverez dans le repertoire *img* des visuels de la solution.

<img width="1722" alt="Screenshot 2023-02-28 at 10 15 04 PM" src="https://user-images.githubusercontent.com/62513048/221998698-b95d1af6-feb4-41de-9105-2f3131774822.png">
<img width="1722" alt="Screenshot 2023-02-28 at 10 14 19 PM" src="https://user-images.githubusercontent.com/62513048/221998711-319320dc-8202-495d-b8b0-570db62b3633.png">
<img width="1722" alt="Screenshot 2023-02-28 at 10 14 09 PM" src="https://user-images.githubusercontent.com/62513048/221998722-a1636478-4550-4f8f-a41d-4592c8b88c9f.png">
<img width="1722" alt="Screenshot 2023-02-28 at 10 13 58 PM" src="https://user-images.githubusercontent.com/62513048/221999178-5a8d9ebb-54b1-4b3c-80bc-d5550301c7bc.png">
<img width="1722" alt="Screenshot 2023-02-28 at 6 57 39 PM" src="https://user-images.githubusercontent.com/62513048/221999216-e52f3cfc-7025-4066-94b6-8f59d6a5a42f.png">


# Perspectives d'amélioration

- Ajout de tests unitaires
- Amélioration du modèle d'analyse de sentiments
- Utilisation de spark pour le traitement des données