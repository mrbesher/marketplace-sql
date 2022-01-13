/* TRIGGER PROCs */
create or replace function is_occupied_proc ()
    returns trigger
    as $$
begin
    if new.seller_id is null then
        return new;
    end if;
    if old.seller_id is not null then
        raise exception 'location is already occupied';
        return old;
    else
        return new;
    end if;
end;
$$
language 'plpgsql';

create or replace function pos_quantity_proc ()
    returns trigger
    as $$
begin
    if old.quantity < 0 then
        raise exception 'quantity cannot be negative';
        return old;
    else
        return new;
    end if;
end;
$$
language 'plpgsql';


/* TRIGGERS */
create or replace trigger is_occupied before update on location for each row execute procedure is_occupied_proc ();

create or replace trigger pos_quantity before update on stock for each row execute procedure pos_quantity_proc ();


/* TYPES - RECORDS */
do $$
begin
    if not exists (
        select
            1
        from
            pg_type
        where
            typname = 'standrecord') then
    create type standrecord as (
        nmemb int,
        new_stand int
);
end if;
end
$$;


/* SEQUENCES */
create sequence if not exists productid_seq;

create sequence if not exists sellerid_seq;

create sequence if not exists saleid_seq;


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
    price real not null,
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
    v_msg text;
    declare loc_cur cursor for
        select
            *
        from
            generate_series(1, n);
begin
    for loc in loc_cur loop
        insert into location (id, seller_id)
            values (loc.generate_series, null);
    end loop;
    exception
        when others then
            get stacked diagnostics v_msg = message_text;
    raise notice E'%', v_msg;
    return;
    end;

$$
language 'plpgsql';


/* initialize locations */
select
    init_locations (100);


/* VIEWS */
create or replace view sellers_loc as
select
    s.id as sellerid,
    s.seller_name as fname,
    s.seller_surname as lname,
    l.id as loc
from
    seller s,
    location l
where
    s.id = l.seller_id;


/* FUNCTIONS */
/* ==adding entities== */
create or replace function add_product (name varchar(200), out productId int)
as $$
declare
    v_msg text;
    next_pid int;
begin
    next_pid := nextval('productid_seq');
    insert into product (id, product_name)
        values (next_pid, name);
    productId := next_pid;
exception
    when others then
        get stacked diagnostics v_msg = message_text;
    raise notice E'%', v_msg;
    perform
        setval('productid_seq', next_pid, false);
    productId := - 1;
end;

$$
language 'plpgsql';

create or replace function add_stock (sellerId int, productId int, quant int, pr real)
    returns int
    as $$
declare
    v_msg text;
begin
    insert into stock (seller_id, product_id, quantity, price)
        values (sellerId, productId, quant, pr);
    return 0;
exception
    when others then
        get stacked diagnostics v_msg = message_text;
    raise notice E'%', v_msg;
    return - 1;
end;

$$
language 'plpgsql';

create or replace function add_user (username varchar(200), password varchar(200), loc int, fname varchar(200), surname varchar(200))
    returns int
    as $$
declare
    v_msg text;
    stand standrecord;
begin
    stand.new_stand := nextval('sellerid_seq');
    insert into seller (id, seller_name, seller_surname)
        values (stand.new_stand, fname, surname);
    insert into market_user (id, password, seller_id)
        values (username, password, stand.new_stand);

    /* add user to location */
    stand.nmemb := (
        select
            count(*)
        from
            location l
        where
            id = loc
            and seller_id is null);
    if stand.nmemb < 1 then
        raise exception 'LOCATION OCCUPIED --> %', loc
            using HINT = 'Please try another location';
        end if;
        update
            location
        set
            seller_id = stand.new_stand
        where
            id = loc
            and seller_id is null;
        return 0;
exception
    when others then
        get stacked diagnostics v_msg = message_text;
    raise notice E'%', v_msg;
    perform
        setval('sellerid_seq', stand.new_stand, false);
    return - 1;
end;

$$
language 'plpgsql';


/* ==removing entities== */
create or replace function remove_user (sellerId int)
    returns int
    as $$
declare
    v_msg text;
begin
    /* remove location referencing user */
    update
        location l
    set
        seller_id = null
    where
        l.seller_id = sellerId;

    /* remove user */
    delete from market_user m
    where m.seller_id = sellerId;

    /* remove seller */
    delete from seller s
    where s.id = sellerId;
    return 0;
exception
    when others then
        get stacked diagnostics v_msg = message_text;
    raise notice E'%', v_msg;
    return - 1;
end;

$$
language 'plpgsql';

create or replace function remove_stock (sellerId int, productId int)
    returns int
    as $$
declare
    v_msg text;
begin
    delete from stock s
    where s.seller_id = sellerId
        and s.product_id = productId;
exception
    when others then
        get stacked diagnostics v_msg = message_text;
    raise notice E'%', v_msg;
    return - 1;
end;

$$
language 'plpgsql';


/* ==listing== */
create or replace function list_products_not_in_stock (sellerId int)
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
            stock.product_id
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

create or replace function list_sellers ()
    returns table (
        sellerid int,
        fname varchar(200),
        lname varchar(200),
        loc int
    )
    as $$
begin
    return query
    select
        sl.sellerid,
        sl.fname,
        sl.lname,
        sl.loc
    from
        sellers_loc sl;
end;
$$
language 'plpgsql';

create or replace function list_available_loc ()
    returns table (
        loc int
    )
    as $$
begin
    return query
    select
        id
    from
        location
    where
        location.seller_id is null;
end;
$$
language 'plpgsql';

create or replace function list_sales_usr (sellerId int, starting date, ending date)
    returns table (
        productid int,
        quantity int
    )
    as $$
begin
    return query
    select
        sale.product_id,
        sum(sale.quantity)::int
    from
        sale
    where
        sale.seller_id = sellerId
        and sale.sale_date >= starting
        and sale.sale_date <= ending
    group by
        sale.product_id
    having
        sum(sale.quantity) > 0;
end;
$$
language 'plpgsql';

create or replace function list_stock (sellerId int)
    returns table (
        productid int,
        quantity int,
        price real
    )
    as $$
begin
    return query
    select
        stock.product_id,
        stock.quantity,
        stock.price
    from
        stock
    where
        stock.seller_id = sellerId;
end;
$$
language 'plpgsql';

create or replace function list_all_products (sellerId int)
    returns table (
        productid int,
        productname varchar(200)
    )
    as $$
begin
    return query
    select
        p.id,
        p.product_name
    from
        product p,
        stock s
    where
        s.product_id = p.id
        and s.seller_id = sellerId;
end;
$$
language 'plpgsql';

create or replace function list_product_stocks (productId int)
    returns table (
        sellername varchar(200),
        loc int,
        quantity int,
        price real
    )
    as $$
begin
    return query
    select
        s.seller_name,
        l.id,
        st.quantity,
        st.price
    from
        stock st,
        seller s,
        location l
    where
        st.seller_id = s.id
        and l.seller_id = s.id
        and st.quantity > 0;
end;
$$
language 'plpgsql';


/* ==routines== */
create or replace function mylogin (username varchar(200), pass varchar(200))
    returns int
    as $$
declare
    v_msg text;
    sellerid bigint;
begin
    sellerid := (
        select
            seller.id
        from
            market_user,
            seller
        where
            market_user.seller_id = seller.id
            and market_user.id = username
            and market_user.password = pass);
    if sellerid is null then
        return - 1;
    end if;
    return sellerid;
exception
    when others then
        get stacked diagnostics v_msg = message_text;
    raise notice E'%', v_msg;
    return - 1;
end;

$$
language 'plpgsql';

create or replace function update_quantity (sellerId int, productId int, quantityChange int)
    returns int
    as $$
declare
    v_msg text;
    curr_quantity int;
begin
    curr_quantity := (
        select
            quantity
        from
            stock
        where
            stock.seller_id = sellerId
            and stock.product_id = productId);
    curr_quantity := curr_quantity + quantityChange;
    if curr_quantity is null or curr_quantity < 0 then
        return - 1;
    end if;
    update
        stock
    set
        stock.quantity = curr_quantity
    where
        stock.seller_id = sellerId
        and stock.product_id = productId;
    return 0;
exception
    when others then
        get stacked diagnostics v_msg = message_text;
    raise notice E'%', v_msg;
    return - 1;
end;

$$
language 'plpgsql';

create or replace function sell (sellerId int, productId int, q int)
    returns int
    as $$
declare
    v_msg text;
    curr_quantity int;
    curr_date date;
begin
    curr_quantity := (
        select
            quantity
        from
            stock
        where
            stock.seller_id = sellerId
            and stock.product_id = productId);
    curr_quantity := curr_quantity - q;
    if curr_quantity is null or curr_quantity < 0 then
        raise notice 'no sufficent quantity or seller does not sell product';
        return - 1;
    end if;
    update
        stock
    set
        quantity = curr_quantity
    where
        stock.seller_id = sellerId
        and stock.product_id = productId;
    curr_date := current_date;
    insert into sale (id, seller_id, product_id, quantity, sale_date)
        values (nextval('saleid_seq'), sellerId, productId, q, curr_date);
    return 0;
exception
    when others then
        get stacked diagnostics v_msg = message_text;
    raise notice E'%', v_msg;
    return - 1;
end;

$$
language 'plpgsql';

