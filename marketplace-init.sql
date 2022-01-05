/* SEQUENCES */
create sequence if not exists productid_seq;

create sequence if not exists sellerid_seq;


/* TABLES */
create table if not exists seller (
    id int primary key,
    seller_name varchar(200),
    seller_surname varchar(200)
);

create table if not exists location (
    id int primary key,
    seller_id int references seller (id) on delete cascade
);

create table if not exists product (
    id int primary key,
    product_name varchar(200) unique
);

create table if not exists sale (
    id int primary key,
    seller_id int not null references seller (id) on delete cascade,
    product_id int not null references product (id) on delete cascade,
    quantity int not null,
    sale_date date not null
);

create table if not exists stock (
    seller_id int references seller (id) on delete cascade,
    product_id int references product (id) on delete cascade,
    quantity int not null,
    price int not null,
    constraint pk_stock primary key (product_id, seller_id),
    check (price > 0),
    check (quantity > - 1)
);

create table if not exists market_user (
    id varchar(200) unique,
    password varchar(200),
    seller_id int references seller (id) on delete cascade
);


/* SET n LOCATIONS WITH THEIR IDS */
create or replace function init_locations (n int)
    returns void
    as $$
declare
    loc_cur cursor for
        select
            *
        from
            generate_series(1, n);
begin
    for loc in loc_cur loop
        insert into location (id, seller_id)
            values (loc, null);
    end loop;
end;
$$
language 'plpgsql';


/* initialize locations */
select
    init_locations (100);


/* VIEWS */
/* FUNCTIONS */
/* adding entities */
create or replace function add_product (name varchar(200), out product_id int)
as $$
declare
    next_pid int;
begin
    next_pid := nextval('productid_seq');
    insert into product (id, product_name)
        values (next_pid, name);
    product_id := next_pid;
exception
    when others then
        perform
            setval('productid_seq', next_pid, false);
    product_id := - 1;
end;

$$
language 'plpgsql';

create or replace function add_stock (seller_id int, product_id int, quantity int, price int)
    returns int
    as $$
begin
    insert into stock (seller_id, product_id, quantity, price)
        values (seller_id, product_id, quantity, price);
    return 0;
exception
    when others then
        return - 1;
end;

$$
language 'plpgsql';

create or replace function add_user (username varchar(200), password varchar(200), loc int, fname varchar(200), surname varchar(200))
    returns int
    as $$
declare
    next_sid int;
    satis int;
begin
    next_sid := nextval('sellerid_seq');
    insert into seller (id, seller_name, seller_surname)
        values (next_sid, fname, surname);
    insert into market_user (id, password, seller_id)
        values (username, password, next_sid);

    /* add user to location */
    satis := (
        select
            count(*)
        from
            location l
        where
            id = loc
            and seller_id is null);
    if satis < 1 then
        raise exception 'LOCATION OCCUPIED --> %', loc
            using HINT = 'Please try another location';
    end if;
    
    update
        location
    set
        seller_id = next_sid
    where
        id = loc
        and seller_id is null;
    return 0;
exception
    when others then
        perform
            setval('sellerid_seq', next_sid, false);
    return - 1;
end;

$$
language 'plpgsql';


/* removing entities */
create or replace function remove_user (seller_id int)
    returns int
    as $$
begin
    /* remove location referencing user */
    update
        location l
    set
        seller_id = null
    where
        l.seller_id = seller_id;

    /* remove user */
    delete from market_user m
    where m.seller_id = seller_id;

    /* remove seller */
    delete from seller s
    where s.id = seller_id;
    return 0;
exception
    when others then
        return - 1;
end;

$$
language 'plpgsql';


/* listing */
create or replace function list_products_not_in_stock (seller_id int)
    returns table (
        product_id int,
        product_name varchar(200)
    )
    as $$
begin
    return query with not_taken as (
        select
            id
        from
            product
        except
        select
            product_id
        from
            stock
)
    select
        p.id,
        p.product_name
    from
        product p,
        not_taken
    where
        not_taken.id = p.id;
end;
$$
language 'plpgsql';

