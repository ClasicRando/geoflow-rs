-- Custom implmentation of audit-trigger project, https://github.com/2ndQuadrant/audit-trigger
create schema audit;
revoke all on schema audit from public;

comment on schema audit is 'Out-of-table audit/history logging tables and trigger functions';

create type audit.audit_action as enum('I','D','U','T');

create table audit.logged_actions (
    event_id bigint primary key generated always as identity,
    schema_name text not null,
    table_name text not null,
    relid oid not null,
    session_user_name text,
    action_tstamp_tx timestamp with time zone not null,
    action_tstamp_stm timestamp with time zone not null,
    action_tstamp_clk timestamp with time zone not null,
    transaction_id bigint,
    application_name text,
    client_addr inet,
    client_port integer,
    client_query text,
    action audit.audit_action not null,
    row_data jsonb,
    changed_fields jsonb,
    statement_only boolean not null,
    geoflow_user_id bigint,
);

revoke all on audit.logged_actions from public;

comment on table audit.logged_actions is 'History of auditable actions on audited tables, from audit.if_modified_func()';
comment on column audit.logged_actions.event_id is 'Unique identifier for each auditable event';
comment on column audit.logged_actions.schema_name is 'Database schema audited table for this event is in';
comment on column audit.logged_actions.table_name is 'Non-schema-qualified table name of table event occured in';
comment on column audit.logged_actions.relid is 'Table OID. Changes with drop/create. Get with ''tablename''::regclass';
comment on column audit.logged_actions.session_user_name is 'Login / session user whose statement caused the audited event';
comment on column audit.logged_actions.action_tstamp_tx is 'Transaction start timestamp for tx in which audited event occurred';
comment on column audit.logged_actions.action_tstamp_stm is 'Statement start timestamp for tx in which audited event occurred';
comment on column audit.logged_actions.action_tstamp_clk is 'Wall clock time at which audited event''s trigger call occurred';
comment on column audit.logged_actions.transaction_id is 'Identifier of transaction that made the change. May wrap, but unique paired with action_tstamp_tx.';
comment on column audit.logged_actions.client_addr is 'IP address of client that issued query. Null for unix domain socket.';
comment on column audit.logged_actions.client_port is 'Remote peer IP port address of client that issued query. Undefined for unix socket.';
comment on column audit.logged_actions.client_query is 'Top-level query that caused this auditable event. May be more than one statement.';
comment on column audit.logged_actions.application_name is 'Application name set when this audit event occurred. Can be changed in-session by client.';
comment on column audit.logged_actions.action is 'Action type; I = insert, D = delete, U = update, T = truncate';
comment on column audit.logged_actions.row_data is 'Record value. Null for statement-level trigger. For INSERT this is the new tuple. For DELETE and UPDATE it is the old tuple.';
comment on column audit.logged_actions.changed_fields is 'New values of fields changed by UPDATE. Null except for row-level UPDATE events.';
comment on column audit.logged_actions.statement_only is '''t'' if audit event is from an FOR EACH STATEMENT trigger, ''f'' for FOR EACH ROW';

create index logged_actions_relid_idx on audit.logged_actions(relid);
create index logged_actions_action_tstamp_tx_stm_idx on audit.logged_actions(action_tstamp_stm);
create index logged_actions_action_idx on audit.logged_actions(action);

create function audit.if_modified_func()
returns trigger
language plpgsql
security definer
set search_path = pg_catalog, public
as $$
declare
    audit_row audit.logged_actions;
    excluded_cols text[] := ARRAY[]::text[];
begin
    if TG_WHEN != 'AFTER' then
        raise exception 'audit.if_modified_func() may only run as an AFTER trigger';
    end if;

    audit_row := row(
        -1,                                           -- event_id
        TG_TABLE_SCHEMA::text,                        -- schema_name
        TG_TABLE_NAME::text,                          -- table_name
        TG_RELID,                                     -- relation OID for much quicker searches
        session_user::text,                           -- session_user_name
        current_timestamp,							  -- action_tstamp_tx
        statement_timestamp(),                        -- action_tstamp_stm
        clock_timestamp(),                            -- action_tstamp_clk
        txid_current(),                               -- transaction ID
        current_setting('application_name'),          -- client application
        inet_client_addr(),                           -- client_addr
        inet_client_port(),                           -- client_port
        current_query(),                              -- top-level query or queries (if multistatement) from client
        substring(TG_OP,1,1)::audit.audit_action,     -- action
        null, null,                                   -- row_data, changed_fields
        'f',                                          -- statement_only
        nullif(current_setting('geoflow.uid', true),'') -- geoflow_user_id
    );

    if not TG_ARGV[0]::boolean is distinct from 'f'::boolean then
        audit_row.client_query := null;
    end if;

    if TG_ARGV[1] is not null then
        excluded_cols := TG_ARGV[1]::text[];
    end if;

    if TG_OP = 'UPDATE' and TG_LEVEL = 'ROW' then
        audit_row.row_data := to_jsonb(OLD.*) - excluded_cols;
        select jsonb_object_agg(new_row.key, new_row.value)
        into   audit_row.changed_fields
        from   jsonb_each_text(to_jsonb(NEW)) new_row
        join   jsonb_each_text(audit_row.row_data) old_row on new_row.key = old_row.key
        where  new_row.value is distinct from old_row.value;

        if audit_row.changed_fields = '{}'::jsonb then
            -- All changed fields are ignored. Skip this update.
            return null;
        end if;
    elsif TG_OP = 'DELETE' and TG_LEVEL = 'ROW' then
        audit_row.row_data = to_jsonb(OLD.*) - excluded_cols;
    elsif TG_OP = 'INSERT' and TG_LEVEL = 'ROW' then
        audit_row.row_data = to_jsonb(NEW.*) - excluded_cols;
    elsif TG_LEVEL = 'STATEMENT' and TG_OP in ('INSERT','UPDATE','DELETE','TRUNCATE') then
        audit_row.statement_only := 't';
    else
        raise exception '[audit.if_modified_func] - Trigger func added as trigger for unhandled case: %, %',TG_OP, TG_LEVEL;
        return null;
    end if;
    insert into audit.logged_actions(
        schema_name,
        table_name,
        relid,
        session_user_name,
        action_tstamp_tx,
        action_tstamp_stm,
        action_tstamp_clk,
        transaction_id,
        application_name,
        client_addr,
        client_port,
        client_query,
        action,
        row_data,
        changed_fields,
        statement_only,
        geoflow_user_id
    )
    values (
        audit_row.schema_name,
        audit_row.table_name,
        audit_row.relid,
        audit_row.session_user_name,
        audit_row.action_tstamp_tx,
        audit_row.action_tstamp_stm,
        audit_row.action_tstamp_clk,
        audit_row.transaction_id,
        audit_row.application_name,
        audit_row.client_addr,
        audit_row.client_port,
        audit_row.client_query,
        audit_row.action,
        audit_row.row_data,
        audit_row.changed_fields,
        audit_row.statement_only,
        audit_row.geoflow_user_id
    );
    return null;
end;
$$;

comment on function audit.if_modified_func() is $$
Track changes to a table at the statement and/or row level.

Optional parameters to trigger in CREATE TRIGGER call:

param 0: boolean, whether to log the query text. Default 't'.

param 1: text[], columns to ignore in updates. Default [].

         Updates to ignored cols are omitted from changed_fields.

         Updates with only ignored cols changed are not inserted
         into the audit log.

         Almost all the processing work is still done for updates
         that ignored. If you need to save the load, you need to use
         WHEN clause on the trigger instead.

         No warning or error is issued if ignored_cols contains columns
         that do not exist in the target table. This lets you specify
         a standard set of ignored columns.

There is no parameter to disable logging of values. Add this trigger as
a 'FOR EACH STATEMENT' rather than 'FOR EACH ROW' trigger if you do not
want to log row values.

Note that the user name logged is the login role for the session. The audit trigger
cannot obtain the active role because it is reset by the SECURITY DEFINER invocation
of the audit trigger its self.
$$;

create or replace procedure audit.audit_table(
    target_table regclass,
    audit_rows boolean default true,
    audit_query_text boolean default true,
    ignored_cols text[] default '{}'::text[]
)
language plpgsql
as $$
declare
    stm_targets text := 'INSERT OR UPDATE OR DELETE OR TRUNCATE';
    _q_txt text;
    _ignored_cols_snip text := ', ' || quote_literal(coalesce($4,'{}'::text[]));
    _schema text;
    _name text;
begin
    if $2 is null or $3 is null then
        raise exception '2nd and 3rd parameters must be non-null';
    end if;

    select n.nspname::text, c.relname::text
    into   _schema, _name
    from   pg_class c
    join   pg_namespace n on c.relnamespace = n.oid
    where  c.oid = target_table::oid;

    execute format('DROP TRIGGER IF EXISTS audit_trigger_row ON %I.%I', _schema, _name);
    execute format('DROP TRIGGER IF EXISTS audit_trigger_stm ON %I.%I', _schema, _name);

    if $2 then
        _q_txt := format(
            'CREATE TRIGGER audit_trigger_row '
            'AFTER INSERT OR UPDATE OR DELETE ON %I.%I '
            'FOR EACH ROW EXECUTE PROCEDURE audit.if_modified_func(' ||
            quote_literal($3) || _ignored_cols_snip || ');',
            _schema,
            _name
        );
        raise notice '%',_q_txt;
        execute _q_txt;
        stm_targets := 'TRUNCATE';
    end if;

    _q_txt := format(
        'CREATE TRIGGER audit_trigger_stm '
        'AFTER ' || stm_targets || ' ON %I.%I '
        'FOR EACH STATEMENT EXECUTE PROCEDURE audit.if_modified_func(' || quote_literal($3) || ');',
        _schema,
        _name
    );
    raise notice '%',_q_txt;
    execute _q_txt;
end;
$$;

comment on procedure audit.audit_table(regclass, boolean, boolean, text[]) IS $$
Add auditing support to a table.

Arguments:
   target_table:     Table name, schema qualified if not on search_path
   audit_rows:       Record each row change, or only audit at a statement level, default is true (i.e. row level)
   audit_query_text: Record the text of the client query that triggered the audit event? default is true
   ignored_cols:     Columns to exclude from update diffs, ignore updates that change only ignored cols. default is none
$$;

create view audit.tableslist as
select distinct triggers.trigger_schema as schema, triggers.event_object_table AS auditedtable
from   information_schema.triggers
where  triggers.trigger_name::text in ('audit_trigger_row'::text, 'audit_trigger_stm'::text)
order by 1, 2;

comment on view audit.tableslist is $$
View showing all tables with auditing set up. Ordered by schema, then table.
$$;

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

call audit.audit_table('geoflow.users');

create table geoflow.user_roles (
    uid bigint not null references geoflow.users (uid) match simple
        on update cascade
        on delete cascade,
    role_id bigint not null references geoflow.roles (role_id) match simple
        on update cascade
        on delete restrict
);

call audit.audit_table('geoflow.user_roles');

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
    call geoflow.validate_password($3);

    insert into geoflow.users(name,username,password)
    values($1,$2,crypt($3, gen_salt('bf')))
    returning uid into v_uid;

    insert into geoflow.user_roles(uid,role_id)
    select v_uid, r
    from   unnest($4) r;

    return v_uid;
end;
$$;

create function geoflow.validate_user(
    username text,
    password text
) returns bigint
stable
language plpgsql
returns null on null input
as $$
declare
    result bigint;
begin
    begin
        select uid
        into   result
        from   geoflow.users
        where  username = $1
        and    password = crypt($2, password);
    exception
        when no_data_found then
            return null;
    end;

    return result;
end;
$$;

create function geoflow.update_user_password(
    username text,
    old_password text,
    new_password text
) returns bigint
volatile
language plpgsql
as $$
declare
    v_uid bigint;
begin
    if geoflow.validate_user($1, $2) is not null then
        raise exception 'Could not validate the old password for the username of "%s"', $1;
    end if;

    call geoflow.validate_password($3);

    update geoflow.users
    set    password = crypt($3, gen_salt('bf'))
    where  username = $1
    returning uid into v_uid;

    return v_uid;
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

create function geoflow.region_change()
returns trigger
language plpgsql
stable
as $$
declare
    v_country text;
    v_prov text;
begin
    begin
        select distinct country_name
        into   v_country
        from   geoflow.regions
        where  country_code = new.country_code;

        if v_country != new.country_name then
            raise exception 'Country name of the current record does not match the name found in the table. Expected "%", found "%"', v_country, new.country_name;
        end if;
    exception
        when no_data_found then
            null;
    end;

    if new.prov_code is null then
        return new;
    end if;

    begin
        select distinct prov_name
        into   v_prov
        from   geoflow.regions
        where  country_code = new.country_code
        and    prov_code = new.prov_code;

        if v_prov != new.prov_name then
            raise exception 'Prov name of the current record does not match the name found in the table. Expected "%", found "%"', v_prov, new.prov_name;
        end if;
    exception
        when no_data_found then
            null;
    end;

    select count(0)
    into   v_check
    from   geoflow.regions
    where  country_code = new.country_code
    and    coalesce(prov_code,'x') = coalesce(new.prov_code,'x')
    and    coalesce(county,'x') = coalesce(new.county,'x');

    if v_check > 0 then
        raise exception 'Attempted to insert a record that already exists. "%", "%", "%"', new.country_code, new.prov_code, new.county;
    end if;

    return new;
end;
$$;

create table geoflow.regions (
    region_id bigint primary key generated always as identity,
    country_code text not null check(geoflow.check_not_blank_or_empty(country_code)),
    country_name text not null check(geoflow.check_not_blank_or_empty(country_name)),
    prov_code text check(geoflow.check_not_blank_or_empty(prov_code)),
    prov_name text check(geoflow.check_not_blank_or_empty(prov_name)),
    county text check(geoflow.check_not_blank_or_empty(county)),
    constraint regions_prov_check check(
        case
            when prov_code is not null and prov_name is null then false
            when prov_code is null and prov_name is not null then false
            else true
        end
    ),
    constraint regions_county_prov_check check(
        case
            when county is not null and prov_code is null then false
            else true
        end
    ),
    constraint regions_unique unique (country_code,prov_code,county)
);

call audit.audit_table('geoflow.regions');

create trigger region_change
    before update or insert
    on geoflow.regions
    for each row
    execute procedure geoflow.region_change();

insert into geoflow.regions(country_code,prov_code,prov_name,country_name)
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
('CA','YT','Yukon','Canada'),
('CA',null,null,'Canada'),
('US',null,null,'United States');

create table geoflow.warehouse_types (
    wt_id integer primary key generated always as identity,
    name text not null check(geoflow.check_not_blank_or_empty(name)),
    description text not null check(geoflow.check_not_blank_or_empty(description))
);

call audit.audit_table('geoflow.warehouse_types');

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
    if TG_OP = 'INSERT' then
        if not geoflow.user_can_create_ds(new.created_by) then
            raise exception 'uid %s cannot create a new data source. Check user roles to enable data source creation.', new.created_by;
        end if;
        new.updated_by := new.created_by;
        new.last_updated := now();
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
    last_updated timestamp not null,
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

call audit.audit_table('geoflow.data_sources');

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

create procedure geoflow.update_data_source(
    geoflow_user_id bigint,
    ds_id bigint,
    name text,
    description text,
    search_radius real,
    comments text,
    region_id bigint,
    warehouse_type integer,
    collection_workflow bigint,
    load_workflow bigint,
    check_workflow bigint
)
language sql
as $$
update geoflow.data_sources
set    updated_by = $1,
       name = $3,
       description = $4,
       search_radius = $5,
       comments = $6,
       region_id = $7,
       warehouse_type = $8,
       collection_workflow = $9,
       load_workflow = $10,
       check_workflow = $11
where  ds_id = $2;
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

call audit.audit_table('geoflow.data_source_contacts');

create trigger data_source_contact_change
    before update or insert
    on geoflow.data_source_contacts
    for each row
    execute procedure geoflow.data_source_contacts_change();

create type geoflow.data_source_contact as
(
    contact_id bigint,
    name text,
    email text,
    website text,
    contact_type text,
    notes text,
    created timestamp without time zone,
    created_by text,
    last_updated timestamp without time zone,
    updated_by text
);

create view geoflow.v_data_sources as
    with contacts as (
        select ds_id,
               array_agg(row(
                dsc.contact_id,
                dsc.name,
                dsc.email,
                dsc.website,
                dsc.type,
                dsc.notes,
                dsc.created,
                u1.name,
                dsc.last_updated,
                u2.name
               )::geoflow.data_source_contact) contacts
        from   geoflow.data_source_contacts dsc
        join   geoflow.users u1 on u1.uid = dsc.created_by
        join   geoflow.users u2 on u2.uid = dsc.updated_by
        group by dsc.ds_id
    )
    select ds.ds_id, ds.name, ds.description, ds.search_radius, ds.comments,
           r.region_id, r.country_code, r.country_name, r.prov_code, r.prov_name, r.county,
           u1.name as "assigned_user", ds.created, u2.name as "created_by", ds.last_updated, u3.name as "updated_by",
           wt.wt_id, wt.name as "warehouse_name", wt.description as "warehouse_description",
           ds.collection_workflow, ds.load_workflow, ds.check_workflow,
           coalesce(c.contacts,'{}'::geoflow.data_source_contact[]) as "contacts"
    from   geoflow.data_sources ds
    join   geoflow.regions r on r.region_id = ds.region_id
    join   geoflow.users u1 on u1.uid = ds.assigned_user
    join   geoflow.users u2 on u2.uid = ds.created_by
    join   geoflow.users u3 on u3.uid = ds.updated_by
    join   geoflow.warehouse_types wt on wt.wt_id = ds.warehouse_type
    left join contacts c on c.ds_id = ds.ds_id;

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

call audit.audit_table('geoflow.load_instances');

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
language plpgsql
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
    to_load boolean not null default true,
    loaded_timestamp timestamp with time zone,
    error_message text check(geoflow.check_not_blank_or_empty(error_message)),
    constraint source_data_load_instance_table_name unique (li_id, table_name),
    constraint source_data_load_source_id unique (li_id, load_source_id)
);
create index source_data_li_id on geoflow.source_data(li_id);

create function geoflow.get_source_data_entry(sd_id bigint)
returns geoflow.source_data
stable
language sql
as $$
select sd_id, li_id, load_source_id, user_generated, options, table_name, columns,
       to_load, loaded_timestamp, error_message
from   geoflow.source_data
where  sd_id = $1
$$;

create function geoflow.get_source_data(li_id bigint)
returns setof geoflow.source_data
stable
language sql
as $$
select sd_id, li_id, load_source_id, user_generated, options, table_name, columns,
       to_load, loaded_timestamp, error_message
from   geoflow.source_data
where  li_id = $1
$$;

create function geoflow.get_source_data_to_load(workflow_run_id bigint)
returns setof geoflow.source_data
stable
language sql
as $$
with load_instance as (
    select li_id
    from   geoflow.load_instances
    where  load_workflow_run_id = $1
)
select sd_id, li_id, load_source_id, user_generated, options, table_name, columns,
       to_load, loaded_timestamp, error_message
from   geoflow.source_data
where  li_id in (select li_id from load_instance)
and    to_load
$$;

create function geoflow.create_source_data_entry(
    geoflow_user_id bigint,
    li_id bigint,
    user_generated boolean,
    options jsonb,
    table_name text,
    columns geoflow.column_metadata[],
    out sd_id bigint,
    out load_source_id smallint
)
volatile
language plpgsql
as $$
begin
    if not geoflow.user_can_update_ls($1, $2) then
        raise exception 'uid %s cannot create a new source data entry. User must be part of the load instance.', $1;
    end if;
    insert into geoflow.source_data(li_id,user_generated,options,table_name,columns)
    values($2,$3,$4,$5,$6)
    returning sd_id, load_source_id into $7, $8;
end;
$$;

create function geoflow.update_source_data_entry(
    geoflow_user_id bigint,
    sd_id bigint,
    li_id bigint,
    user_generated boolean,
    options jsonb,
    table_name text,
    columns geoflow.column_metadata[]
) returns geoflow.source_data
volatile
language plpgsql
returns null on null input
as $$
declare
    result geoflow.source_data;
begin
    if not geoflow.user_can_update_ls($1, $3) then
        raise exception 'uid %s cannot create a new source data entry. User must be part of the load instance.', $1;
    end if;

    update geoflow.source_data
    set    load_source_id = $3,
           user_generated = $4,
           options = $5,
           table_name = $6,
           columns = $7
    where  sd_id = $2
    returning sd_id, li_id, load_source_id, user_generated, options, table_name, columns,
              to_load, loaded_timestamp, error_message
    into result;
    return result;
end;
$$;

create function geoflow.delete_source_data_entry(
    geoflow_user_id bigint,
    sd_id bigint
) returns geoflow.source_data
volatile
language plpgsql
returns null on null input
as $$
declare
    result geoflow.source_data;
begin
    if not geoflow.user_can_update_ls($1, $2) then
        raise exception 'uid %s cannot create a new source data entry. User must be part of the load instance.', $1;
    end if;

    delete from geoflow.source_data
    where  sd_id = $2
    returning sd_id, li_id, load_source_id, user_generated, options, table_name, columns,
              to_load, loaded_timestamp, error_message
    into result;
    return result;
end;
$$;

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

call audit.audit_table('geoflow.plotting_fields');

create schema bulk_loading;
revoke all on schema bulk_loading from public;

comment on schema bulk_loading is 'Destination of all bulk loaded source data.';
