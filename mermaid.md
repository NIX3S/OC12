erDiagram
    AFP_FACTUEL {
        id TEXT
        title TEXT
        text TEXT
        image_url TEXT
        label TEXT
        language TEXT
        retrieved_at DATETIME
    }

    LEGORAFI {
        id TEXT
        title TEXT
        text TEXT
        image_url TEXT
        publication_date DATETIME
        label TEXT
        language TEXT
        retrieved_at DATETIME
    }

    NEWSAPI {
        id TEXT
        title TEXT
        text TEXT
        image_url TEXT
        publication_date DATETIME
        source TEXT
        language TEXT
        retrieved_at DATETIME
    }

    NEWSDATA {
        id TEXT
        title TEXT
        text TEXT
        image_url TEXT
        publication_date DATETIME
        source TEXT
        language TEXT
        retrieved_at DATETIME
    }

    DB_GLOBALE {
        TEXT id PK "Identifiant unique (URL ou hash)"
        TEXT source "Nom du média ou agrégateur"
        TEXT title "Titre de l’article"
        TEXT text "Contenu textuel nettoyé"
        TEXT image_url "Lien vers l’image principale"
        DATETIME publication_date "Date de publication normalisée"
        TEXT domain "Nom de domaine du site source"
        TEXT language "Langue principale du texte"
        TEXT label "Étiquette de véracité (fake, satire, real, etc.)"
        INTEGER text_length "Taille du texte (nombre de caractères)"
        INTEGER has_label "Indicateur binaire : 1 = label présent"
        BOOLEAN image_valid "URL d’image valide (HTTP 200)"
        DATETIME retrieved_at "Horodatage de collecte"
    }

    AFP_FACTUEL ||--|| DB_GLOBALE : "fusion et normalisation"
    LEGORAFI    ||--|| DB_GLOBALE : "fusion et enrichissement"
    NEWSAPI     ||--|| DB_GLOBALE : "intégration API JSON"
    NEWSDATA    ||--|| DB_GLOBALE : "agrégation multilingue"
