-- drop attic
DROP TABLE user_events;
DROP TABLE agg_count;
DROP VIEW IF EXISTS mv_agg_count;

-- 1. Создание 2 физических таблиц - для raw логов и lля агрегаций под mv, + сама mv
CREATE TABLE user_events (
    user_id UInt32,
    event_type String,
    points_spent UInt32,
    event_time DateTime
) 
ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(event_time) 
ORDER BY (event_time, user_id)  
TTL event_time + INTERVAL 30 DAY;  -- Автоудаление данных старше 30 дней

CREATE TABLE agg_count (
	event_date Date,
	event_type String,
    uniqUsers AggregateFunction(uniqState, UInt32),
    sumPoints AggregateFunction(sumState, UInt32),
    cntActions AggregateFunction(countState)
    )
ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMMDD(event_date) 
ORDER BY (event_date, event_type)
TTL event_date + INTERVAL 180 DAY; -- Автоудаление данных старше 180 дней

CREATE MATERIALIZED VIEW mv_agg_count
TO agg_count AS
SELECT
    toDate(event_time) AS event_date, 
	event_type,
    uniqState(user_id) AS uniqUsers, 
    sumState(points_spent) AS sumPoints, 
    countState() AS cntActions
FROM user_events
GROUP BY event_date, event_type;

-- 1.1 Наполнение таблицы с логами
INSERT INTO user_events VALUES
(1, 'login', 0, now() - INTERVAL 10 DAY),
(2, 'signup', 0, now() - INTERVAL 10 DAY),
(3, 'login', 0, now() - INTERVAL 10 DAY),
(1, 'login', 0, now() - INTERVAL 7 DAY),
(2, 'login', 0, now() - INTERVAL 7 DAY),
(3, 'purchase', 30, now() - INTERVAL 7 DAY),
(1, 'purchase', 50, now() - INTERVAL 5 DAY),
(2, 'logout', 0, now() - INTERVAL 5 DAY),
(4, 'login', 0, now() - INTERVAL 5 DAY),
(1, 'login', 0, now() - INTERVAL 3 DAY),
(3, 'purchase', 70, now() - INTERVAL 3 DAY),
(5, 'signup', 0, now() - INTERVAL 3 DAY),
(2, 'purchase', 20, now() - INTERVAL 1 DAY),
(4, 'logout', 0, now() - INTERVAL 1 DAY),
(5, 'login', 0, now() - INTERVAL 1 DAY),
(1, 'purchase', 25, now()),
(2, 'login', 0, now()),
(3, 'logout', 0, now()),
(6, 'signup', 0, now()),
(6, 'purchase', 100, now());

-- 1.2 check
select * from user_events;
select count(*) from agg_count;

-- 2. Запрос на агрегацию по дням и по типам событий
select
    toDate(event_time) as event_date, 
	event_type,
    uniqExact(user_id) as uniq_users, 
    sum(points_spent) as total_spent, 
    count() as total_actions
from user_events
group by event_date, event_type
order by event_date, event_type;

-- 3. Запрос на определение куммулятивного retention 7d
with
    user_installs as ( -- Найдём дату первого события каждого юзера
        select
            user_id,
            toDate(min(event_time)) as install_date
        from user_events
        group by user_id
    ),
    user_returns as ( -- Присоединим события и вычислим, вернулся ли пользователь в течение 7 дней после установки
        select
            e.user_id,
            ui.install_date,
            toDate(min(e.event_time)) AS first_return_date
        from user_events e
        inner join user_installs ui using user_id
        where e.event_time > ui.install_date -- не 1ое а 2+ касание
          and e.event_time <= ui.install_date + interval 7 DAY
        group by e.user_id, ui.install_date
    )
select -- ну и считаем кумулятивный ретеншн
    install_date,
    count(distinct ui.user_id) as total_users_day_0,
    count(distinct ur.user_id) as returned_in_7_days,
    round(100.0 * returned_in_7_days / total_users_day_0, 2) AS retention_7d_percent
from user_installs ui
left join user_returns ur using user_id
-- на случай если нужны только те даты инсталов, для которых окно в +7 дней завершилось уже
-- а то получается в результате невалидные сопоставления дат.
-- но т.к. активация следующего условия оставляет в результирующей таблице только 1 дату 
-- (в виду малого кол-ва сырых данных о логах), то принял решение просто оставить замьюченым условие
-- where install_date <= today() - INTERVAL 8 DAY 
group by install_date
order by install_date;