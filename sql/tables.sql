CREATE TABLE IF NOT EXISTS retina_nations (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    delegacy TEXT DEFAULT NULL,
    region TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS retina_endorsements (
    endorser TEXT NOT NULL,
    target TEXT NOT NULL,
    PRIMARY KEY (endorser, target)
);

CREATE INDEX IF NOT EXISTS retina_nations_region_idx ON retina_nations (region);
CREATE INDEX IF NOT EXISTS retina_endos_endorser_idx ON retina_endorsements (endorser);
CREATE INDEX IF NOT EXISTS retina_endos_combined_idx ON retina_endorsements (target, endorser);