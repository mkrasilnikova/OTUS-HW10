
drop table if exists orders_info;
drop table if exists orders_info_current_day;

create table if not exists orders_info
(
    order_date TIMESTAMP not null,
    order_number bigint not null,
    total_order_price numeric not null,
    created_at timestamp with time zone not null default current_timestamp
);

create table if not exists orders_info_current_day
(
    order_date TIMESTAMP not null,
    order_number bigint not null,
    total_order_price numeric not null,
    created_at timestamp with time zone not null default current_timestamp
);


