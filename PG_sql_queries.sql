
--  1. Создание таблиц
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    name TEXT,
    email TEXT,
    role TEXT,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE users_audit (
    id SERIAL PRIMARY KEY,
    user_id INTEGER,
    changed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    changed_by TEXT,
    field_changed TEXT,
    old_value TEXT,
    new_value TEXT
);

CREATE EXTENSION IF NOT EXISTS pg_cron;

select * from homework.users;
select * from homework.users_audit;

--  2. TRIGGER 
-- Создаю функцию логирования изменений по трем полям (может и больше = любые)
CREATE OR REPLACE FUNCTION log_user_changes()
RETURNS TRIGGER AS $$
DECLARE  -- объявляю используемые далее внутри функции локальные переменные 
	col TEXT;    
	old_val TEXT;
    new_val TEXT;
BEGIN
    FOR col IN SELECT column_name
               FROM information_schema.columns
               WHERE table_schema = 'homework' and table_name = 'users'
                 AND column_name NOT IN ('id', 'updated_at')  -- исключаем id и служебное поле
    LOOP
        EXECUTE format('SELECT ($1).%I::TEXT', col) INTO old_val USING OLD; -- Получение старого и нового значений динамически
        EXECUTE format('SELECT ($1).%I::TEXT', col) INTO new_val USING NEW;

        IF old_val IS DISTINCT FROM new_val THEN -- Проверка: было ли изменение
            INSERT INTO users_audit (user_id, changed_at, changed_by, field_changed, old_value, new_value)
            VALUES (OLD.id, CURRENT_TIMESTAMP, current_user, col, old_val, new_val);
        END IF;
    END LOOP;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Сам Триггер на таблицу users
CREATE TRIGGER trigger_users_audit_update
BEFORE UPDATE ON users
FOR EACH ROW
EXECUTE FUNCTION log_user_changes();

-- 3.Функция экспорта свежих изменений в CSV
CREATE OR REPLACE FUNCTION export_users_audit()
RETURNS VOID AS $$
DECLARE
    filename TEXT;
BEGIN
    filename := '/tmp/users_audit_export_' || TO_CHAR(NOW(), 'YYYY-MM-DD') || '.csv';

    EXECUTE FORMAT(
        'COPY (SELECT * FROM users_audit WHERE changed_at >= CURRENT_DATE) 
        TO %L WITH CSV HEADER',
        filename
    );
END;
$$ LANGUAGE plpgsql;

-- 4. Внесение первичных данных в таблицу (инициализация)
INSERT INTO homework.users (name, email, role)
VALUES 
('Ivan Ivanov', 'ivan@example.com', 'data_analyst'),
('Anna Petrova', 'anna@example.com', 'system_analyst'),
('Olga Sidorova', 'olga@example.com', 'data_engineer');

-- select * from homework.users;
-- select * from homework.users_audit;

-- 5. Update данных в таблицe users
UPDATE homework.users
SET role = 'data_engineer'
WHERE name = 'Ivan Ivanov';

-- select * from homework.users;
-- select * from homework.users_audit;

UPDATE homework.users
SET email = 'anna_new@example.com'
WHERE name = 'Anna Petrova';

-- select * from homework.users;
-- select * from homework.users_audit;


-- 6. Cron
SELECT cron.schedule('0 3 * * *', 'SELECT export_users_audit();');

-- 7. Manual init
SELECT export_users_audit();