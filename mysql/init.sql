-- =============================================================================
-- PROJET INDUTECH - Schema MySQL pour la persistance des tickets
-- =============================================================================

CREATE DATABASE IF NOT EXISTS indutech
  CHARACTER SET utf8mb4
  COLLATE utf8mb4_unicode_ci;

USE indutech;

-- Tickets enrichis (une ligne = un ticket consommé depuis Redpanda)
CREATE TABLE IF NOT EXISTS tickets (
    ticket_id       VARCHAR(36)  NOT NULL,
    client_id       INT          NOT NULL,
    created_at      VARCHAR(40),
    event_time      DATETIME,
    demande         TEXT,
    type            VARCHAR(32),
    priorite        VARCHAR(16),
    equipe_support  VARCHAR(32),
    urgent          BOOLEAN,
    inserted_at     TIMESTAMP    DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (ticket_id),
    INDEX idx_type (type),
    INDEX idx_priorite (priorite),
    INDEX idx_equipe (equipe_support),
    INDEX idx_event_time (event_time)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Agrégations par type et par fenêtre glissante de 1 minute
CREATE TABLE IF NOT EXISTS tickets_agg_par_type (
    window_start    DATETIME     NOT NULL,
    window_end      DATETIME     NOT NULL,
    type            VARCHAR(32)  NOT NULL,
    equipe_support  VARCHAR(32),
    nb_tickets      BIGINT,
    inserted_at     TIMESTAMP    DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_window (window_start, window_end),
    INDEX idx_type (type)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Droits (par sécurité, MySQL 8 crée déjà le user via env vars)
GRANT ALL PRIVILEGES ON indutech.* TO 'indutech'@'%';
FLUSH PRIVILEGES;
