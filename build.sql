create schema geoflow;

CREATE FUNCTION geoflow.check_not_blank_or_empty(
	text
) RETURNS boolean
LANGUAGE plpgsql
IMMUTABLE
LANGUAGE plpgsql
AS $$
BEGIN
	RETURN COALESCE($1,'x') !~ '^\s*$';
END;
$$;

CREATE FUNCTION geoflow.check_array_not_blank_or_empty(
	text[]
) RETURNS boolean
IMMUTABLE
LANGUAGE plpgsql
AS $$
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
$$;

CREATE FUNCTION geoflow.check_timestamp_later(
	to_check timestamp,
    other timestamp
) RETURNS boolean
IMMUTABLE
LANGUAGE plpgsql
AS $$
BEGIN
    RETURN CASE
        WHEN $1 IS NULL THEN true
        WHEN $2 IS NULL THEN false
        ELSE $1 > $2
    END;
END;
$$;

create table geoflow.roles (
	role_id integer primary key generated always as identity,
	name text not null check(geoflow.check_not_blank_or_empty(name)) unique,
	description text not null check(geoflow.check_not_blank_or_empty(description))
);

create table geoflow.users (
	uid bigint primary key generated always as identity,
	name text not null check(geoflow.check_not_blank_or_empty(name)),
	username text not null check(geoflow.check_not_blank_or_empty(username)) unique,
    password text not null
);

create table geoflow.user_roles (
	uid bigint not null references geoflow.users (uid) match simple
        on update cascade
        on delete cascade,
	role_id bigint not null references geoflow.roles (role_id) match simple
        on update cascade
        on delete restrict
);

create table geoflow.regions (
	region_id bigint primary key generated always as identity,
    country_code text not null check(geoflow.check_not_blank_or_empty(country_code)),
	country_name text not null check(geoflow.check_not_blank_or_empty(country_name)),
    prov_code text check(geoflow.check_not_blank_or_empty(prov_code)),
    prov_name text check(geoflow.check_not_blank_or_empty(prov_name)),
    county text check(geoflow.check_not_blank_or_empty(county))
);

insert into geoflow.regions(country_code,prov_code,name,country_name)
values('US','AL','Alabama','United States'),
('US','AK','Alaska','United States'),
('US','AZ','Arizona','United States'),
('US','AR','Arkansas','United States'),
('US','CA','California','United States'),
('US','CO','Colorado','United States'),
('US','CT','Connecticut','United States'),
('US','DE','Delaware','United States'),
('US','DC','District of Columbia','United States'),
('US','FL','Florida','United States'),
('US','GA','Georgia','United States'),
('US','HI','Hawaii','United States'),
('US','ID','Idaho','United States'),
('US','IL','Illinois','United States'),
('US','IN','Indiana','United States'),
('US','IA','Iowa','United States'),
('US','KS','Kansas','United States'),
('US','KY','Kentucky','United States'),
('US','LA','Louisiana','United States'),
('US','ME','Maine','United States'),
('US','MD','Maryland','United States'),
('US','MA','Massachusetts','United States'),
('US','MI','Michigan','United States'),
('US','MN','Minnesota','United States'),
('US','MS','Mississippi','United States'),
('US','MO','Missouri','United States'),
('US','MT','Montana','United States'),
('US','NE','Nebraska','United States'),
('US','NV','Nevada','United States'),
('US','NH','New Hampshire','United States'),
('US','NJ','New Jersey','United States'),
('US','NM','New Mexico','United States'),
('US','NY','New York','United States'),
('US','NC','North Carolina','United States'),
('US','ND','North Dakota','United States'),
('US','OH','Ohio','United States'),
('US','OK','Oklahoma','United States'),
('US','OR','Oregon','United States'),
('US','PA','Pennsylvania','United States'),
('US','RI','Rhode Island','United States'),
('US','SC','South Carolina','United States'),
('US','SD','South Dakota','United States'),
('US','TN','Tennessee','United States'),
('US','TX','Texas','United States'),
('US','UT','Utah','United States'),
('US','VT','Vermont','United States'),
('US','VA','Virginia','United States'),
('US','WA','Washington','United States'),
('US','WV','West Virginia','United States'),
('US','WI','Wisconsin','United States'),
('US','WY','Wyoming','United States'),
('US','AS','American Samoa','United States'),
('US','GU','Guam','United States'),
('US','MP','Northern Mariana Islands','United States'),
('US','PR','Puerto Rico','United States'),
('US','UM','U.S. Minor Outlying Islands','United States'),
('US','VI','U.S. Virgin Islands','United States'),
('CA','AB','Alberta','Canada'),
('CA','BC','British Columbia','Canada'),
('CA','SK','Saskatchewan','Canada'),
('CA','MB','Manitoba','Canada'),
('CA','NB','New Brunswick','Canada'),
('CA','NL','Newfoundland and Labrador','Canada'),
('CA','NT','Northwest Territories','Canada'),
('CA','NS','Nova Scotia','Canada'),
('CA','NU','Nunavut','Canada'),
('CA','ON','Ontario','Canada'),
('CA','PE','Prince Edward Island','Canada'),
('CA','QC','Quebec','Canada'),
('CA','YT','Yukon','Canada');

create table geoflow.warehouse_types (
	wt_id integer primary key generated always as identity,
	name text not null check(geoflow.check_not_blank_or_empty(name)),
	description text not null check(geoflow.check_not_blank_or_empty(description))
);

insert into geoflow.warehouse_types(name,description)
values('Current', 'Only keep the current dataset. All non-matched records are deleted.'),
('Archive', 'Production contains current dataset only but non-matched records are moved to a separate archive.'),
('Historical', 'Keep all records. Current dataset updates production record details when matched and non-matched records are retained.'),
('Full Historical', 'Keep all records. Current dataset merges with production record details when matched and non-matched records are retained.');

create table geoflow.data_sources (
	ds_id bigint primary key generated always as identity,
	name text not null check(geoflow.check_not_blank_or_empty(name)),
	description text not null check(geoflow.check_not_blank_or_empty(description)),
    search_radius real not null check(search_radius > 0),
    comments text,
    region_id bigint not null references geoflow.regions (region_id) match simple
        on update cascade
        on delete restrict,
    assinged_user bigint not null references geoflow.users (uid) match simple
        on update cascade
        on delete restrict,
    created timestamp not null default timezone('utc'::text, now()),
    created_by bigint not null references geoflow.users (uid) match simple
        on update cascade
        on delete set null,
    last_updated timestamp,
    updated_by bigint not null references geoflow.users (uid) match simple
        on update cascade
        on delete set null,
    warehouse_type integer not null references geoflow.warehouse_types (wt_id) match simple
        on update cascade
        on delete restrict,
    collection_workflow bigint not null check(collection_workflow > 0),
    load_workflow bigint not null check(load_workflow > 0),
    check_workflow bigint not null check(check_workflow > 0)
);

create table geoflow.data_source_contacts (
    contact_id bigint primary key generated always as identity,
	ds_id bigint not null references geoflow.data_sources (ds_id) match simple
        on update cascade
        on delete restrict,
	name text not null check(geoflow.check_not_blank_or_empty(name)),
	email text check(geoflow.check_not_blank_or_empty(email)),
	website text check(geoflow.check_not_blank_or_empty(website)),
	type text check(geoflow.check_not_blank_or_empty(type)),
	notes text check(geoflow.check_not_blank_or_empty(notes))
);

create type geoflow.load_state as enum ('Active', 'Ready', 'Hold');
create type geoflow.merge_type as enum ('None', 'Exclusive', 'Intersect');

create table geoflow.load_instances (
	li_id bigint primary key generated always as identity,
	ds_id bigint not null references geoflow.data_sources (ds_id) match simple
        on update cascade
        on delete restrict,
	version_date date not null,
    collect_user_id bigint references geoflow.users (uid) match simple
        on update cascade
        on delete set null,
    load_user_id bigint references geoflow.users (uid) match simple
        on update cascade
        on delete set null,
    check_user_id bigint references geoflow.users (uid) match simple
        on update cascade
        on delete set null,
    state geoflow.load_state not null default 'Ready'::geoflow.load_state,
    production_count integer not null default 0 check(production_count >= 0),
    staging_count integer not null default 0 check(staging_count >= 0),
    match_count integer not null default 0 check(match_count >= 0),
    new_count integer not null default 0 check(new_count >= 0),
    plotting_stats jsonb not null default '{}'::jsonb,
	collect_start timestamp check(geoflow.check_timestamp_later(collect_finish, collect_start)),
	collect_finish timestamp check(geoflow.check_timestamp_later(collect_finish, collect_start)),
	collect_workflow_id bigint not null,
	collect_workflow_run_id bigint,
	load_start timestamp check(geoflow.check_timestamp_later(load_finish, load_start)),
	load_finish timestamp check(geoflow.check_timestamp_later(load_finish, load_start)),
	load_workflow_id bigint not null,
	load_workflow_run_id bigint,
	check_start timestamp check(geoflow.check_timestamp_later(check_finish, check_start)),
	check_finish timestamp check(geoflow.check_timestamp_later(check_finish, check_start)),
	check_workflow_id bigint not null,
	check_workflow_run_id bigint,
	done timestamp,
    merge_type geoflow.merge_type not null
);

create function geoflow.init_load_instance(
    ds_id bigint,
    version_date date
) returns bigint
stable
LANGUAGE sql
returns null on null input
as $$
insert into geoflow.load_instances(ds_id,version_date,collect_workflow_id,load_workflow_id,check_workflow_id,merge_type)
with last_merge_type as (
	select ds_id, merge_type
	from   geoflow.load_instances
	where  ds_id = $1
	order by li_id desc
	limit 1
)
select ds.ds_id, $2, ds.collection_workflow, ds.load_workflow, ds.check_workflow, coalesce(lmt.merge_type,'None'::geoflow.merge_type)
from   geoflow.data_sources ds
left join last_merge_type lmt on ds.ds_id = lmt.ds_id
where  ds.ds_id = $1
returning li_id;
$$;

create table geoflow.plotting_method_types (
	pmt_id integer primary key generated always as identity,
	name text not null check(geoflow.check_not_blank_or_empty(name)) unique,
	description text not null check(geoflow.check_not_blank_or_empty(description))
);

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
	geoflow.column_metadata[]
) RETURNS boolean
LANGUAGE 'plpgsql'
IMMUTABLE
AS $$
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
$$;

create table geoflow.source_data (
	sd_id bigint primary key generated always as identity,
	li_id bigint not null references geoflow.load_instances (li_id) match simple
        on update cascade
        on delete restrict,
    file_id text not null check(file_id ~ '^[FU]\d+$'),
	options jsonb not null,
	table_name text not null check(table_name ~ '^[A-Z_][A-Z_0-9]{1,64}$'),
    columns geoflow.column_metadata[] not null check(geoflow.valid_column_metadata(columns)),
	constraint source_data_load_instance_table_name unique (li_id, table_name)
);
create index source_data_li_id on geoflow.source_data(li_id);

create function geoflow.plotting_methods_change()
returns trigger
language plpgsql
stable
as $$
declare
    check_count bigint;
begin
    select count(distinct sd_id)
    into   check_count
    from   new_table;
    
    if check_count > 1 then
        raise exception 'Cannot insert for multiple sd_id';
    end if;
    
    select count(0)
    into   check_count
    from (
        select plotting_order, row_number() over (order by plotting_order) rn
        from   new_table
    ) t1
    where  rn != plotting_order;
    
    if check_count > 0 then
        raise exception 'The order of the plotting methods has skipped a value';
    end if;
    return null;
end;
$$;

create table geoflow.plotting_methods (
	sd_id bigint not null references geoflow.source_data (sd_id) match simple
        on update cascade
        on delete restrict,
	plotting_order smallint not null check(plotting_order > 0),
    method_type integer not null references geoflow.plotting_method_types (pmt_id) match simple
        on update cascade
        on delete restrict,
    constraint plotting_methods_pk primary key (sd_id, plotting_order)
);

create trigger plotting_methods_insert_trigger
    after insert
    on geoflow.plotting_methods
    referencing new table as new_table
    for each statement
    execute function geoflow.plotting_methods_change();

create trigger plotting_methods_update_trigger
    after update
    on geoflow.plotting_methods
    referencing new table as new_table
    for each statement
    execute function geoflow.plotting_methods_change();

create table geoflow.plotting_fields (
    sd_id bigint not null references geoflow.source_data (sd_id) match simple
        on update cascade
        on delete restrict,
    location_name text check (geoflow.check_not_blank_or_empty(location_name)),
    address_line1 text check (geoflow.check_not_blank_or_empty(address_line1)),
    address_line2 text check (geoflow.check_not_blank_or_empty(address_line2)),
    city text check (geoflow.check_not_blank_or_empty(city)),
    alternate_cities text[] check (geoflow.check_array_not_blank_or_empty(alternate_cities)),
    mail_code text check (geoflow.check_not_blank_or_empty(mail_code)),
    latitude text check (geoflow.check_not_blank_or_empty(latitude)),
    longitude text check (geoflow.check_not_blank_or_empty(longitude)),
    prov text check (geoflow.check_not_blank_or_empty(prov)),
    clean_address text check (geoflow.check_not_blank_or_empty(clean_address)),
    clean_city text check (geoflow.check_not_blank_or_empty(clean_city)),
    constraint plotting_fields_pk primary key (sd_id)
);
