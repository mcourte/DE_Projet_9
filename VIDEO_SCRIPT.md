# Script de la vidéo de démonstration — POC InduTech

**Durée cible :** ~5 minutes
**Format :** screencast + voix off (Loom / YouTube)
**Résolution :** 1920×1080, zoom sur le terminal pour la lisibilité

---

## Séquençage

| # | Durée  | Plan                                  | Objectif                        |
|---|--------|---------------------------------------|---------------------------------|
| 1 | 0:00–0:30 | Titre + contexte                   | Poser le sujet                  |
| 2 | 0:30–1:00 | Diagramme Mermaid du README        | Expliquer l'architecture        |
| 3 | 1:00–1:30 | Structure du projet (VS Code)      | Montrer les fichiers clés       |
| 4 | 1:30–2:15 | `docker compose up` Redpanda+MySQL | Démarrer l'infrastructure       |
| 5 | 2:15–3:00 | Lancement du producer              | Générer des tickets             |
| 6 | 3:00–3:50 | Lancement du consumer PySpark      | Traiter le flux                 |
| 7 | 3:50–4:30 | Vérification MySQL + Parquet       | Montrer les sorties             |
| 8 | 4:30–5:00 | Conclusion + arrêt propre          | Récapituler                     |

---

## Script détaillé (voix off)

### 1. Intro — 0:00 → 0:30

> « Bonjour, je suis Marion Courté. Dans cette vidéo, je vais vous présenter
> le POC de gestion de tickets clients réalisé pour **InduTech**, dans le
> cadre du Projet 9 du parcours Data Engineer.
>
> L'objectif : démontrer un pipeline de **streaming temps réel** qui génère
> des tickets, les fait transiter par un broker **Redpanda**, les analyse
> en continu avec **PySpark Structured Streaming**, et persiste les
> résultats dans **MySQL**. »

**À l'écran :** slide de titre → logo / nom du projet.

---

### 2. Architecture — 0:30 → 1:00

> « Voici l'architecture. Un script Python joue le rôle de **producteur** :
> il génère des tickets avec Faker et les publie sur le topic
> `client_tickets`. Redpanda, compatible Kafka, fait office de broker.
> Côté consommation, un job **PySpark** lit le flux, enrichit chaque ticket
> avec une équipe de support et un flag d'urgence, puis écrit les résultats
> dans **deux tables MySQL** — `tickets` et `tickets_agg_par_type` — avec
> une copie en Parquet pour l'analyse froide. »

**À l'écran :** diagramme Mermaid du README (zoom progressif).

---

### 3. Structure du projet — 1:00 → 1:30

> « Le projet contient quatre briques : le dossier `producer/` avec son
> script, le dossier `spark/` pour le consommateur PySpark, le dossier
> `mysql/` avec le script `init.sql` qui crée le schéma, et le
> `docker-compose.yml` qui orchestre Redpanda, la Console et MySQL.
> Les dépendances Python sont dans `requierements.txt` :
> `kafka-python-ng`, `faker` et `pyspark`. »

**À l'écran :** VS Code, arborescence à gauche, ouvrir brièvement
`producer.py`, `pyspark_consumer.py`, `mysql/init.sql`, `docker-compose.yml`.

---

### 4. Démarrage de l'infrastructure — 1:30 → 2:15

> « Premier lancement : l'infrastructure. J'exécute `docker compose up -d`
> pour démarrer trois services : le nœud Redpanda sur le port 19092, la
> Console Redpanda sur 8080, et MySQL 8 sur 3306. MySQL exécute
> automatiquement le script `init.sql` qui crée la base `indutech` et les
> deux tables cibles. »

**Commandes à l'écran :**
```bash
docker compose up -d
docker compose ps
```

> « Je crée ensuite le topic `client_tickets` : »
```bash
docker exec -it redpanda-0 rpk topic create client_tickets
```

> « Et je vérifie que MySQL est bien prêt avec ses tables : »
```bash
docker exec -it indutech-mysql mysql -uindutech -pindutech indutech -e "SHOW TABLES;"
```

**À l'écran :** split terminal + Redpanda Console (http://localhost:8080).

---

### 5. Producer — 2:15 → 3:00

> « Je lance maintenant le producteur. Il génère un ticket toutes les 1 à
> 5 secondes, avec un UUID, un client_id, un timestamp ISO8601, une
> demande générée par Faker, un type et une priorité. »

**Commande :**
```bash
python producer/producer.py
```

> « Chaque ligne de log confirme l'envoi, avec le partition et l'offset
> retournés par le broker. Dans la Redpanda Console, les messages
> apparaissent en temps réel dans le topic. »

**À l'écran :** split terminal + console Redpanda onglet "Messages".

---

### 6. Consumer PySpark — 3:00 → 3:50

> « Dans un second terminal, je lance le job Spark avec `spark-submit` et
> **deux packages** : le connecteur Kafka et le driver JDBC MySQL. »

**Commande :**
```bash
spark-submit \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,com.mysql:mysql-connector-j:8.4.0 \
    spark/pyspark_consumer.py
```

> « Spark démarre, se connecte au broker et à MySQL. Le script fait quatre
> choses :
> 1. il désérialise le JSON et applique le schéma ;
> 2. il enrichit chaque ticket — `technique` devient l'équipe `Tech`,
>    `facturation` l'équipe `Finance`, `livraison` la `Logistique` ;
> 3. il agrège les volumes par type sur des fenêtres glissantes d'une
>    minute, avec un watermark de deux minutes ;
> 4. il écrit les résultats dans MySQL via `foreachBatch`, avec un
>    **retry exponentiel de 5 tentatives** pour absorber les déconnexions
>    JDBC. Si une écriture échoue définitivement, le checkpoint Spark
>    garantit que le micro-batch sera rejoué au redémarrage. »

**À l'écran :** logs Spark, micro-batch qui affiche les tickets enrichis
et les logs `[OK] batch=... tickets ecrits`.

---

### 7. Vérification — 3:50 → 4:30

> « Je vérifie directement dans MySQL que les données arrivent bien. »

**Commandes :**
```bash
docker exec -it indutech-mysql mysql -uindutech -pindutech indutech \
  -e "SELECT type, equipe_support, COUNT(*) FROM tickets GROUP BY type, equipe_support;"
```

> « Les tickets sont répartis par type et par équipe, exactement comme
> attendu. Je regarde aussi la table d'agrégation par fenêtre d'une
> minute : »

```bash
docker exec -it indutech-mysql mysql -uindutech -pindutech indutech \
  -e "SELECT * FROM tickets_agg_par_type ORDER BY window_start DESC LIMIT 10;"
```

> « Et en backup, les mêmes données sont disponibles en Parquet dans
> `/tmp/indutech/output`, pour un éventuel usage analytique froid. »

---

### 8. Conclusion — 4:30 → 5:00

> « Pour résumer : on a un pipeline complet de streaming — génération,
> broker Redpanda, consommation PySpark avec enrichissement et agrégation,
> persistance dans MySQL et backup Parquet — le tout entièrement
> conteneurisé et paramétrable via variables d'environnement, avec une
> gestion de la résilience sur les déconnexions.
>
> Pour arrêter proprement : `Ctrl+C` sur le producer et le consumer, puis
> `docker compose down` pour stopper Redpanda et MySQL.
>
> Merci de votre attention, le code complet est disponible sur le repo
> GitHub, lien en description. »

**À l'écran :** commande d'arrêt + slide finale avec le lien du repo.

---

## Check-list avant l'enregistrement

- [ ] Fermer les applications inutiles, masquer les notifications.
- [ ] Police du terminal en 14 pt minimum, thème sombre contrasté.
- [ ] Résolution écran 1920×1080, ratio 16:9.
- [ ] Micro testé, filtre anti-bruit activé.
- [ ] Pré-build des images Docker pour éviter les downloads en direct.
- [ ] Producer pré-exécuté une fois pour que la base MySQL contienne déjà
      quelques lignes au moment de la démo Spark.
- [ ] Script Spark testé de bout en bout sur la même machine.
- [ ] Vérifier que MySQL est bien `healthy` avant la démo Spark.
- [ ] Découpage en chapitres YouTube (timecodes du tableau ci-dessus).

---

## Intégration dans le README

Une fois la vidéo publiée, ajouter dans le `README.md`, après la section
**Architecture du pipeline** :

```markdown
## Démo vidéo

[![Démo POC InduTech](https://img.youtube.com/vi/VIDEO_ID/maxresdefault.jpg)](https://youtu.be/VIDEO_ID)

> Démo de ~5 min : démarrage du broker et de MySQL, génération de tickets,
> consommation PySpark et vérification des tables MySQL + Parquet.
```

Remplacer `VIDEO_ID` par l'identifiant YouTube (ou un lien Loom direct si
hébergement Loom).
