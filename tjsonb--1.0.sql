
CREATE TYPE tjsonb (
    INTERNALLENGTH = variable,
    INPUT = tjsonb_in,
    OUTPUT = tjsonb_out
);


-- Overlaps func
CREATE FUNCTION tjsonb_overlaps(tjsonb, tjsonb) RETURNS boolean
    AS 'MODULE_PATHNAME', 'tjsonb_overlaps' LANGUAGE C IMMUTABLE STRICT;

-- Aggregation func for average speed
CREATE FUNCTION tjsonb_aggregate_speed(tjsonb[]) RETURNS float8
    AS 'MODULE_PATHNAME', 'tjsonb_aggregate_speed' LANGUAGE C IMMUTABLE STRICT;

-- Range detection func
CREATE FUNCTION tjsonb_range(tjsonb, tjsonb) RETURNS text
    AS 'MODULE_PATHNAME', 'tjsonb_range' LANGUAGE C IMMUTABLE STRICT;

