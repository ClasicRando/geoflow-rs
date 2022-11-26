create schema geoflow;

CREATE FUNCTION geoflow.check_not_blank_or_empty(
	text)
    RETURNS boolean
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
BEGIN
	RETURN COALESCE($1,'x') !~ '^\s*$';
END;
$BODY$;

CREATE FUNCTION geoflow.check_array_not_blank_or_empty(
	text[])
    RETURNS boolean
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
DECLARE
	val text;
BEGIN
	IF $1 = '{}' THEN
		RETURN false;
	END IF;
    IF $1 IS NOT NULL THEN
      FOREACH val IN ARRAY $1
      LOOP
          IF COALESCE(val,'x') ~ '^\s*$' THEN
              RETURN false;
          END IF;
      END LOOP;
    END IF;
	RETURN true;
END;
$BODY$;

create type geoflow.column_type as enum (
    'Text', 'Boolean', 'SmallInt', 'Integer', 'BigInt', 'Number', 'Real', 'DoublePrecision', 'Money',
    'Timestamp', 'TimestampWithZone', 'Date', 'Time', 'Interval', 'Geometry', 'Json', 'UUID', 'SmallIntArray'
);

CREATE TYPE geoflow.column_metadata AS
(
	name text,
	column_type geoflow.column_type
);

CREATE FUNCTION geoflow.valid_column_metadata(
	geoflow.column_metadata[])
    RETURNS boolean
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
DECLARE
	meta geoflow.column_metadata;
BEGIN
	IF $1 = '{}' THEN
		RETURN false;
	END IF;
    FOREACH meta IN ARRAY $1
    LOOP
        IF meta.name IS NULL OR NOT geoflow.check_not_blank_or_empty(meta.name) OR meta.column_type IS NULL THEN
            RETURN false;
        END IF;
    END LOOP;
    RETURN true;
END;
$BODY$;

create table geoflow.source_data (
	sd_id bigint primary key generated always as identity,
	options jsonb not null,
    columns geoflow.column_metadata[] not null check(geoflow.valid_column_metadata(columns))
);
