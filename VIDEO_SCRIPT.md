# Script de la vidéo de démonstration — POC InduTech

**Durée cible :** ~5 minutes
**Format :** screencast + voix off (Loom / YouTube)
**Résolution :** 1920×1080, zoom sur le terminal pour la lisibilité

---

## Séquençage

| # | Durée  | Plan                              | Objectif                        |
|---|--------|-----------------------------------|---------------------------------|
| 1 | 0:00–0:30 | Titre + contexte               | Poser le sujet                  |
| 2 | 0:30–1:00 | Diagramme Mermaid du README    | Expliquer l'architecture        |
| 3 | 1:00–1:30 | Structure du projet (VS Code)  | Montrer les fichiers clés       |
| 4 | 1:30–2:15 | `docker compose up` + Console  | Démarrer le broker              |
| 5 | 2:15–3:00 | Lancement du producer          | Générer des tickets             |
| 6 | 3:00–4:00 | Lancement du consumer PySpark  | Traiter le flux                 |
| 7 | 4:00–4:30 | Vérification Parquet + console | Montrer les sorties             |
| 8 | 4:30–5:00 | Conclusion + arrêt propre      | Récapituler                     |

---

## Script détaillé (voix off)

### 1. Intro — 0:00 → 0:30

> « Bonjour, je suis Marion Courté. Dans cette vidéo, je vais vous présenter
> le POC de gestion de tickets clients réalisé pour **InduTech**, dans le
> cadre du Projet 9 du parcours Data Engineer.
>
> L'objectif : démontrer un pipeline de **streaming temps réel** qui génère
> des tickets, les fait transiter par un broker **Redpanda**, et les analyse
> en continu avec **PySpark Structured Streaming**. »

**À l'écran :** slide de titre → logo / nom du projet.

---

### 2. Architecture — 0:30 → 1:00

> « Voici l'architecture. Un script Python joue le rôle de **producteur** :
> il génère des tickets avec Faker et les publie sur le topic
> `client_tickets`. Redpanda, compatible Kafka, fait office de broker.
> Côté consommation, un job **PySpark** lit le flux, enrichit chaque ticket
> avec une équipe de support et un flag d'urgence, puis écrit les résultats
> en Parquet — à la fois ligne à ligne et en agrégations fenêtrées. »

**À l'écran :** diagramme Mermaid du README (zoom progressif).

---

### 3. Structure du projet — 1:00 → 1:30

> « Le projet contient trois briques : le dossier `producer/` avec son
> script et son Dockerfile, le dossier `spark/` pour le consommateur
> PySpark, et le `docker-compose.yml` qui orchestre Redpanda. Le fichier
> `requierements.txt` liste les dépendances : `kafka-python-ng`, `faker`
> et `pyspark`. »

**À l'écran :** VS Code, arborescence à gauche, ouvrir brièvement
`producer.py`, `pyspark_consumer.py`, `docker-compose.yml`.

---

### 4. Démarrage de Redpanda — 1:30 → 2:15

> « Premier lancement : le broker. J'exécute `docker compose up -d`. Deux
> services démarrent : le nœud Redpanda, qui expose Kafka sur le port
> 19092, et la Redpanda Console, accessible sur `localhost:8080`. »

**Commandes à l'écran :**
```bash
docker compose up -d
docker compose ps
```

> « Je crée ensuite le topic `client_tickets` : »
```bash
docker exec -it redpanda-0 rpk topic create client_tickets
docker exec -it redpanda-0 rpk topic list
```

> « J'ouvre la console web — on voit le topic, encore vide. »

**À l'écran :** http://localhost:8080, onglet Topics.

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
> retournés par le broker. Si je rafraîchis la Redpanda Console, les
> messages apparaissent en temps réel dans le topic. »

**À l'écran :** split terminal + console Redpanda onglet "Messages".

---

### 6. Consumer PySpark — 3:00 → 4:00

> « Dans un second terminal, je lance le job Spark avec `spark-submit` et
> le connecteur Kafka. »

**Commande :**
```bash
spark-submit \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
    spark/pyspark_consumer.py
```

> « Spark démarre, se connecte au broker, lit le flux. Le script fait
> trois choses :
> 1. il désérialise le JSON et applique le schéma ;
> 2. il enrichit chaque ticket — `technique` devient l'équipe `Tech`,
>    `facturation` l'équipe `Finance`, `livraison` la `Logistique` ;
> 3. il agrège les volumes par type sur des fenêtres glissantes d'une
>    minute, avec un watermark de deux minutes.
>
> Les résultats sortent en Parquet et un aperçu est imprimé sur la
> console toutes les 30 secondes. »

**À l'écran :** logs Spark, micro-batch qui affiche les tickets enrichis.

---

### 7. Vérification — 4:00 → 4:30

> « Les fichiers Parquet sont écrits dans `/tmp/indutech/output`. Je peux
> les inspecter rapidement avec pandas… »

**Commande :**
```bash
ls /tmp/indutech/output/tickets/
python -c "import pandas as pd; print(pd.read_parquet('/tmp/indutech/output/tickets').head())"
```

> « Et voilà nos tickets persistés, avec les colonnes enrichies
> `equipe_support` et `urgent`. Même vérification pour l'agrégation par
> type et par fenêtre. »

---

### 8. Conclusion — 4:30 → 5:00

> « Pour résumer : on a un pipeline complet de streaming — génération,
> broker, consommation et persistance — entièrement conteneurisé et
> paramétrable via variables d'environnement.
>
> Pour arrêter proprement : `Ctrl+C` sur le producer et le consumer, puis
> `docker compose down` pour stopper Redpanda.
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
- [ ] Producer pré-exécuté une fois pour que le topic contienne déjà
      quelques messages au moment de la démo Spark.
- [ ] Script Spark testé de bout en bout sur la même machine.
- [ ] Découpage en chapitres YouTube (timecodes du tableau ci-dessus).

---

## Intégration dans le README

Une fois la vidéo publiée, ajouter dans le `README.md`, après la section
**Architecture du pipeline** :

```markdown
## Démo vidéo

[![Démo POC InduTech](https://img.youtube.com/vi/VIDEO_ID/maxresdefault.jpg)](https://youtu.be/VIDEO_ID)

> Démo de ~5 min : démarrage du broker, génération de tickets,
> consommation PySpark et vérification des sorties Parquet.
```

Remplacer `VIDEO_ID` par l'identifiant YouTube (ou un lien Loom direct si
hébergement Loom).
