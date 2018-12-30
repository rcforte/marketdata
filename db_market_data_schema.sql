-- Creates the instrument table
create table if not exists tbl_instrument 
(
    instrument_id integer auto_increment primary key,
    instrument_symbol varchar(20) not null unique,
    instrument_name varchar(200) not null,
    gics_sector varchar(200) not null,
    gics_subindustry varchar(200) not null
);

-- Create the instrument returns table
create table if not exists tbl_instrument_price
(
    price_id integer auto_increment primary key,
    instrument_id integer not null,
    price_date date not null,
    open float,
    high float,
    low float,
    close float,
    adj_close float,
    volume integer,

    -- Define the foreign key to instrument_id
    index instrument_id_index(instrument_id),
    foreign key(instrument_id) references tbl_instrument(instrument_id)
    on delete cascade
);
