create schema geoflow;

create function geoflow.check_not_blank_or_empty(
	text
) returns boolean
language plpgsql
immutable
as $$
begin
	return coalesce($1,'x') !~ '^\s*$';
end;
$$;

create function geoflow.check_array_not_blank_or_empty(
	text[]
) returns boolean
immutable
language plpgsql
as $$
declare
	val text;
begin
	if $1 = '{}' then
		return false;
	end if;
    if $1 is not null then
        foreach val in array $1
        loop
            if coalesce(val,'x') ~ '^\s*$' then
                return false;
            end if;
        end loop;
    end if;
	return true;
end;
$$;

create function geoflow.check_timestamp_later(
	to_check timestamp,
    other timestamp
) returns boolean
immutable
language plpgsql
as $$
begin
    return case
        when $1 is null then true
        when $2 is null then false
        else $1 > $2
    end;
end;
$$;

create table geoflow.roles (
	role_id integer primary key generated always as identity,
	name text not null check(geoflow.check_not_blank_or_empty(name)) unique,
	description text not null check(geoflow.check_not_blank_or_empty(description))
);

insert into geoflow.roles(name,description)
values('admin','All privileges granted'),
('collection','Collect data for a load instance'),
('load','Process a data load'),
('check','Check a load instance'),
('create_ls','create a new load instance'),
('create_ds','create a new data source');

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

create view geoflow.v_users as
	with user_roles as (
		select ur.uid, array_agg(r) roles
		from   geoflow.user_roles ur
		join   geoflow.roles r on ur.role_id = r.role_id
		group by ur.uid
	)
	select u.uid, u.name, u.username, ur.roles
	from   geoflow.users u
	join   user_roles ur on u.uid = ur.uid;

create procedure geoflow.validate_password(password text)
language plpgsql
as $$
begin
	if $1 !~ '[A-Z]' then
        raise exception 'password does meet the requirements. Must contain at least 1 uppercase character.';
	end if;
	if $1 !~ '\d' then
        raise exception 'password does meet the requirements. Must contain at least 1 digit character.';
	end if;
	if $1 !~ '\W' then
        raise exception 'password does meet the requirements. Must contain at least 1 non-alphanumberic character.';
	end if;
end;
$$;

create function geoflow.create_user(
    name text,
    username text,
    password text,
    roles bigint[]
) returns bigint
volatile
language plpgsql
returns null on null input
as $$
declare
	v_uid bigint;
begin
    perform geoflow.validate_password($3);
	
    insert into geoflow.users(name,username,password)
    values($1,$2,crypt($3, gen_salt('bf')))
    returning uid into v_uid;
	
	insert into geoflow.user_roles(uid,role_id)
	select nu.uid, v_uid
	from   unnest($4) r;

	return v_uid;
end;
$$;

create function geoflow.validate_user(
    username text,
    password text
) returns boolean
stable
language plpgsql
as $$
declare
    result boolean;
begin
    if $1 is null or $2 is null then
        return false;
    end if;

    begin
        select (password = crypt($2, password))
        into   result
        from   geoflow.users
        where  username = $1;
    exception
        when no_data_found then
            return false;
    end;
    
    return result;
end;
$$;

create procedure geoflow.update_user_password(
    username text,
    password text
)
language plpgsql
as $$
begin
    perform geoflow.validate_password($2);
	
    update geoflow.users
    set    password = crypt($2, gen_salt('bf'))
    where  username = $1;
end;
$$;

create function geoflow.user_is_admin(
    geoflow_user_id bigint
) returns boolean
stable
language sql
as $$
    select exists(
        select 1
        from   geoflow.user_roles ur
        join   geoflow.roles r on ur.role_id = r.role_id
        where  ur.uid = $1
        and    r.name = 'admin'
    );
$$;

create function geoflow.user_can_create_ds(
    geoflow_user_id bigint
) returns boolean
stable
language sql
as $$
    select exists(
        select 1
        from   geoflow.user_roles ur
        join   geoflow.roles r on ur.role_id = r.role_id
        where  ur.uid = $1
        and    r.name in ('admin','create_ds')
    );
$$;

create function geoflow.user_can_update_ds(
    geoflow_user_id bigint
) returns boolean
stable
language sql
as $$
    select exists(
        select 1
        from   geoflow.user_roles ur
        join   geoflow.roles r on ur.role_id = r.role_id
        where  ur.uid = $1
        and    r.name in ('admin','collection')
    );
$$;

create function geoflow.user_can_create_ls(
    geoflow_user_id bigint
) returns boolean
stable
language sql
as $$
    select exists(
        select 1
        from   geoflow.user_roles ur
        join   geoflow.roles r on ur.role_id = r.role_id
        where  ur.uid = $1
        and    r.name in ('admin','create_ls')
    );
$$;

create function geoflow.user_can_collect(
    geoflow_user_id bigint
) returns boolean
stable
language sql
as $$
    select exists(
        select 1
        from   geoflow.user_roles ur
        join   geoflow.roles r on ur.role_id = r.role_id
        where  ur.uid = $1
        and    r.name in ('admin','collection')
    );
$$;

create function geoflow.user_can_load(
    geoflow_user_id bigint
) returns boolean
stable
language sql
as $$
    select exists(
        select 1
        from   geoflow.user_roles ur
        join   geoflow.roles r on ur.role_id = r.role_id
        where  ur.uid = $1
        and    r.name in ('admin','load')
    );
$$;

create function geoflow.user_can_check(
    geoflow_user_id bigint
) returns boolean
stable
language sql
as $$
    select exists(
        select 1
        from   geoflow.user_roles ur
        join   geoflow.roles r on ur.role_id = r.role_id
        where  ur.uid = $1
        and    r.name in ('admin','check')
    );
$$;

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

create function geoflow.data_sources_change()
returns trigger
language plpgsql
stable
as $$
begin
    if TG_OP = 'INSERT' and not geoflow.user_can_create_ds(new.created_by) then
        raise exception 'uid %s cannot create a new data source. Check user roles to enable data source creation.', new.created_by;
    end if;
    if TG_OP = 'UPDATE' then
        if not geoflow.user_can_update_ds(new.updated_by) then
            raise exception 'uid %s cannot update a data source. Check user roles to enable data source updating.', new.updated_by;
        end if;
        new.last_updated := now();
    end if;
end;
$$;

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

create trigger data_source_change
    before update or insert
    on geoflow.data_sources
    for each row
    execute procedure geoflow.data_sources_change();

create function geoflow.init_data_source(
    geoflow_user_id bigint,
    name text,
    description text,
    search_radius real,
    region_id bigint,
    warehouse_type integer,
    collection_workflow bigint,
    load_workflow bigint,
    check_workflow bigint
) returns bigint
volatile
language sql
returns null on null input
as $$
insert into geoflow.data_sources(
    name,description,search_radius,region_id,assinged_user,created_by,warehouse_type,collection_workflow,load_workflow,check_workflow
)
values($2,$3,$4,$5,$1,$1,$6,$7,$8,$9)
returning ds_id;
$$;

create function geoflow.data_source_contacts_change()
returns trigger
language plpgsql
stable
as $$
begin
    if TG_OP = 'INSERT' and not geoflow.user_can_update_ds(new.created_by) then
        raise exception 'uid %s cannot create a new data source contact. Check user roles to enable data source contact creation.', new.created_by;
    end if;
    if TG_OP = 'UPDATE' then
        if not geoflow.user_can_update_ds(new.updated_by) then
            raise exception 'uid %s cannot update a data source contact. Check user roles to enable data source contact updating.', new.updated_by;
        end if;
        new.last_updated := now();
    end if;
end;
$$;

create table geoflow.data_source_contacts (
    contact_id bigint primary key generated always as identity,
	ds_id bigint not null references geoflow.data_sources (ds_id) match simple
        on update cascade
        on delete restrict,
	name text not null check(geoflow.check_not_blank_or_empty(name)),
	email text check(geoflow.check_not_blank_or_empty(email)),
	website text check(geoflow.check_not_blank_or_empty(website)),
	type text check(geoflow.check_not_blank_or_empty(type)),
	notes text check(geoflow.check_not_blank_or_empty(notes)),
    created timestamp not null default timezone('utc'::text, now()),
    created_by bigint not null references geoflow.users (uid) match simple
        on update cascade
        on delete set null,
    last_updated timestamp,
    updated_by bigint references geoflow.users (uid) match simple
        on update cascade
        on delete set null
);

create trigger data_source_contact_change
    before update or insert
    on geoflow.data_source_contacts
    for each row
    execute procedure geoflow.data_source_contacts_change();

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
    merge_type geoflow.merge_type not null,
    created timestamp not null default timezone('utc'::text, now()),
    created_by bigint not null references geoflow.users (uid) match simple
        on update cascade
        on delete set null,
    last_updated timestamp,
    updated_by bigint references geoflow.users (uid) match simple
        on update cascade
        on delete set null
);

create function geoflow.user_can_update_ls(
    geoflow_user_id bigint,
    ls_id bigint
) returns boolean
stable
language sql
as $$
	select (exists(
        select 1
        from   geoflow.load_instances
        where  ls_id = $2
        and   (
            collect_user_id = $1 or
            load_user_id = $1 or
            check_user_id = $1
        )
    )) or geoflow.user_is_admin($1);
$$;

create function geoflow.load_instances_change()
returns trigger
language plpgsql
stable
as $$
begin
    if TG_OP = 'INSERT' and not geoflow.user_can_create_ls(new.created_by) then
        raise exception 'uid %s cannot create a new load instance. Check user roles to enable load instance creation.', new.created_by;
    end if;
    if TG_OP = 'UPDATE' then
        if not geoflow.user_can_update_ls(new.updated_by, new.ls_id) then
            raise exception 'uid %s cannot update a load instance. They are either not part of the load instance or are not an admin.', new.updated_by;
        end if;
        new.last_updated := now();
    end if;
end;
$$;

create trigger load_instance_change
    before update or insert
    on geoflow.load_instances
    for each row
    execute procedure geoflow.load_instances_change();

create function geoflow.init_load_instance(
    ds_id bigint,
    version_date date
) returns bigint
stable
language sql
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

create type geoflow.column_metadata as
(
	name text,
	column_type geoflow.column_type
);

create function geoflow.valid_column_metadata(
	geoflow.column_metadata[]
) returns boolean
language 'plpgsql'
immutable
as $$
declare
	meta geoflow.column_metadata;
begin
	if $1 = '{}' then
		return false;
	end if;
    foreach meta in array $1
    loop
        if meta.name is null or not geoflow.check_not_blank_or_empty(meta.name) or meta.column_type is null then
            return false;
        end if;
    end loop;
    return true;
end;
$$;

create table geoflow.source_data (
	sd_id bigint primary key generated always as identity,
	li_id bigint not null references geoflow.load_instances (li_id) match simple
        on update cascade
        on delete restrict,
    load_source_id smallint not null check (load_source_id > 0),
    user_generated boolean not null default false,
	options jsonb not null,
	table_name text not null check(table_name ~ '^[A-Z_][A-Z_0-9]{1,64}$'),
    columns geoflow.column_metadata[] not null check(geoflow.valid_column_metadata(columns)),
	constraint source_data_load_instance_table_name unique (li_id, table_name),
	constraint source_data_load_source_id unique (li_id, load_source_id)
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
    execute procedure geoflow.plotting_methods_change();

create trigger plotting_methods_update_trigger
    after update
    on geoflow.plotting_methods
    referencing new table as new_table
    for each statement
    execute procedure geoflow.plotting_methods_change();

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
