drop table if exists symbol_candleline;

create table symbol_candleline (
    tradingday                          int                 NOT NULL,
    instrument_id                       varchar(32)         NOT NULL,
    exchange_id                         varchar(32)         NOT NULL,
    local_exchange_id                   varchar(32)         NOT NULL,
    period                              char(1)             NOT NULL,
    open_price                          decimal(15, 4)      NOT NULL,
    close_price                         decimal(15, 4)      NOT NULL,
    highest_price                       decimal(15, 4)      NOT NULL,
    lowest_price                        decimal(15, 4)      NOT NULL,
    backward_adjusted_open_price        decimal(15, 4),
    backward_adjusted_close_price       decimal(15, 4),
    backward_adjusted_highest_price     decimal(15, 4),
    backward_adjusted_lowest_price      decimal(15, 4),
    backward_adjusted_settlement_price  decimal(15, 4)
);


comment on column symbol_candleline.tradingday is                           '交易日';
comment on column symbol_candleline.instrument_id is                        '合约ID';
comment on column symbol_candleline.exchange_id is                          '交易所ID（ISO标准）';
comment on column symbol_candleline.local_exchange_id is                    '交易所ID（本地名称，订阅行情等需要）';
comment on column symbol_candleline.period is                               'K线周期';
comment on column symbol_candleline.open_price is                           '开盘价';
comment on column symbol_candleline.close_price is                          '收盘价';
comment on column symbol_candleline.highest_price is                        '最高价';
comment on column symbol_candleline.lowest_price is                         '最低价';
comment on column symbol_candleline.backward_adjusted_open_price is         '后复权开盘价';
comment on column symbol_candleline.backward_adjusted_close_price is        '后复权收盘价';
comment on column symbol_candleline.backward_adjusted_highest_price is      '后复权最高价';
comment on column symbol_candleline.backward_adjusted_lowest_price is       '后复权最低价';
comment on column symbol_candleline.backward_adjusted_settlement_price is   '后复权结算价';