DROP MATERIALIZED VIEW IF EXISTS models.dvizhenie_denezhnykh_sredstv_p_and_l_ru_ru;
  CREATE MATERIALIZED VIEW models.dvizhenie_denezhnykh_sredstv_p_and_l_ru_ru AS
    WITH
      source AS (
        SELECT
          bu.id_club,
          --bu.id_user,
          p.id_balance_user,
          bu.id_balance AS id_type_balance,
          p.date AS payment_time,
          p.id,
          pt.direct,
          p.id_payment_type,
          pt.method AS id_grp_payment_type,
          pt.profit,
          p.amount,
          p.id_reservation_terminal,
          p.id_subscription_user,
          p.id_reservation_shop,
          --p.id_user_init,
          p.id_parent,
          pt.id_parent AS id_payment_type_parent,
          p.date + (c.time_zone * INTERVAL '1 second') AS payment_time_local,
          CASE
            WHEN (
              p.id_payment_type IN (5, 8, 23, 38, 41, 44, 65, 68, 71, 11, 47, 74) --Игровая выручка 
              OR p.id_payment_type IN (26, 29, 32, 35) --Выручка бара
              OR p.id_payment_type IN (17, 20, 98, 62, 185)
            ) --Пополнение баланса
            AND pt.profit = 1
            AND pt.method = 1 THEN p.amount
            ELSE NULL
          END AS "Наличка",
          CASE
            WHEN (
              p.id_payment_type IN (5, 8, 23, 38, 41, 44, 65, 68, 71, 11, 47, 74) --Игровая выручка 
              OR p.id_payment_type IN (26, 29, 32, 35) --Выручка бара
              OR p.id_payment_type IN (17, 20, 98, 62, 185)
            ) --Пополнение баланса
            AND pt.profit = 1
            AND pt.method = 2 THEN p.amount
            ELSE NULL
          END AS "Безнал",
          CASE
            WHEN (
              p.id_payment_type IN (5, 8, 23, 38, 41, 44, 65, 68, 71, 11, 47, 74) --Игровая выручка 
              OR p.id_payment_type IN (26, 29, 32, 35) --Выручка бара
              OR p.id_payment_type IN (17, 20, 98, 62, 185)
            ) --Пополнение баланса
            AND pt.profit = 1
            AND pt.method = 3 THEN p.amount
            ELSE NULL
          END AS "Онлайн (APP)",
          CASE
            WHEN p.id_payment_type IN (166, 167)
            AND pt.profit = 1 THEN p.amount
            ELSE NULL
          END AS "Продажи подарочных карт (выручка)",
          CASE
            WHEN p.id_payment_type IN (169, 170)
            AND pt.profit = 1 THEN p.amount
            ELSE NULL
          END AS "Оплата мероприятий (выручка)",
          CASE
            WHEN p.id_payment_type IN (172, 173)
            AND pt.profit = 1 THEN p.amount
            ELSE NULL
          END AS "Оплата других доходов (выручка)",
          CASE
            WHEN p.id_payment_type IN (103, 104)
            AND pt.profit = 0 THEN p.amount
            ELSE NULL
          END AS "Зарплата сотрудников (расход)",
          CASE
            WHEN p.id_payment_type IN (106, 107)
            AND pt.profit = 0 THEN p.amount
            ELSE NULL
          END AS "Зарплата менеджеров (расход)",
          CASE
            WHEN p.id_payment_type IN (100, 101)
            AND pt.profit = 0 THEN p.amount
            ELSE NULL
          END AS "Коммунальные услуги (расход)",
          CASE
            WHEN p.id_payment_type IN (115, 116)
            AND pt.profit = 0 THEN p.amount
            ELSE NULL
          END AS "Интернет (расход)",
          CASE
            WHEN p.id_payment_type IN (109, 110)
            AND pt.profit = 0 THEN p.amount
            ELSE NULL
          END AS "Бонусы команде (расход)",
          CASE
            WHEN p.id_payment_type IN (112, 113)
            AND pt.profit = 0 THEN p.amount
            ELSE NULL
          END AS "Маркетинг и реклама (расход)",
          CASE
            WHEN p.id_payment_type IN (118, 119)
            AND pt.profit = 0 THEN p.amount
            ELSE NULL
          END AS "Расходы бара (расход)",
          CASE
            WHEN p.id_payment_type IN (121, 122)
            AND pt.profit = 0 THEN p.amount
            ELSE NULL
          END AS "Уборка (расход)",
          CASE
            WHEN p.id_payment_type IN (124, 125)
            AND pt.profit = 0 THEN p.amount
            ELSE NULL
          END AS "Оплата CRM и цифровых платежей (расход)",
          CASE
            WHEN p.id_payment_type IN (127, 128)
            AND pt.profit = 0 THEN p.amount
            ELSE NULL
          END AS "Банковские услуги (расход)",
          CASE
            WHEN p.id_payment_type IN (130, 131)
            AND pt.profit = 0 THEN p.amount
            ELSE NULL
          END AS "Бухгалтерские услуги (расход)",
          CASE
            WHEN p.id_payment_type IN (133, 134)
            AND pt.profit = 0 THEN p.amount
            ELSE NULL
          END AS "Оборудование (расход)",
          CASE
            WHEN p.id_payment_type IN (136, 137)
            AND pt.profit = 0 THEN p.amount
            ELSE NULL
          END AS "Ремонт (расход)",
          CASE
            WHEN p.id_payment_type IN (139, 140)
            AND pt.profit = 0 THEN p.amount
            ELSE NULL
          END AS "Бытовые расходы (расход)",
          CASE
            WHEN p.id_payment_type IN (142, 143)
            AND pt.profit = 0 THEN p.amount
            ELSE NULL
          END AS "Призовой фонд турниров (расход)",
          CASE
            WHEN p.id_payment_type IN (145, 146)
            AND pt.profit = 0 THEN p.amount
            ELSE NULL
          END AS "Транспортные расходы (расход)",
          CASE
            WHEN p.id_payment_type IN (148, 149)
            AND pt.profit = 0 THEN p.amount
            ELSE NULL
          END AS "Аренда (расход)",
          CASE
            WHEN p.id_payment_type IN (151, 152)
            AND pt.profit = 0 THEN p.amount
            ELSE NULL
          END AS "Юридические расходы (расход)",
          CASE
            WHEN p.id_payment_type IN (154, 155)
            AND pt.profit = 0 THEN p.amount
            ELSE NULL
          END AS "Прочие расходы (расход)",
          CASE
            WHEN p.id_payment_type IN (158, 159)
            AND pt.profit = 0 THEN p.amount
            ELSE NULL
          END AS "Другие расходов (расход)",
          CASE
            WHEN p.id_payment_type IN (160, 161)
            AND pt.profit = 0 THEN p.amount
            ELSE NULL
          END AS "НДС (расход)",
          CASE
            WHEN p.id_payment_type IN (163, 164)
            AND pt.profit = 0 THEN p.amount
            ELSE NULL
          END AS "Налоги и сборы (расход)",
          CASE
            WHEN p.id_payment_type IN (169, 170)
            AND pt.profit = 0 THEN p.amount
            ELSE NULL
          END AS "Оплата мероприятий наличными (расход)",
          CASE
            WHEN p.id_payment_type IN (172, 173)
            AND pt.profit = 0 THEN p.amount
            ELSE NULL
          END AS "Оплата других доходов (расход)"
        FROM
          billing.payment p
          JOIN billing.payment_type pt ON p.id_payment_type = pt.id
          LEFT JOIN billing.balance_user bu ON p.id_balance_user = bu.id
          LEFT JOIN billing.balance b ON bu.id_balance = b.id
          LEFT JOIN cntrl.club AS c ON c.id = bu.id_club
        WHERE
          pt.state = 1
          AND bu.id_balance = 1
      )
    SELECT
      id_club,
      --id_user,
      payment_time,
      payment_time_local,
      id_payment_type,
      id_grp_payment_type,
      'Наличка' AS "Метрика",
      "Наличка"::double precision AS "Значение",
      1.2 AS "Порядок"
    FROM
      source
    WHERE
      "Наличка" IS NOT NULL
    UNION ALL
    SELECT
      id_club,
      --id_user,
      payment_time,
      payment_time_local,
      id_payment_type,
      id_grp_payment_type,
      'Безнал' AS "Метрика",
      "Безнал"::double precision AS "Значение",
      1.3 AS "Безнал"
    FROM
      source
    WHERE
      "Безнал" IS NOT NULL
    UNION ALL
    SELECT
      id_club,
      --id_user,
      payment_time,
      payment_time_local,
      id_payment_type,
      id_grp_payment_type,
      'Онлайн (APP)' AS "Метрика",
      "Онлайн (APP)"::double precision AS "Значение",
      1.4 AS "Онлайн (APP)"
    FROM
      source
    WHERE
      "Онлайн (APP)" IS NOT NULL
    UNION ALL
    SELECT
      id_club,
      --id_user,
      payment_time,
      payment_time_local,
      id_payment_type,
      id_grp_payment_type,
      'Продажи подарочных карт (выручка)' AS "Метрика",
      "Продажи подарочных карт (выручка)"::double precision AS "Значение",
      1.51 AS "Порядок"
    FROM
      source
    WHERE
      "Продажи подарочных карт (выручка)" IS NOT NULL
    UNION ALL
    SELECT
      id_club,
      --id_user,
      payment_time,
      payment_time_local,
      id_payment_type,
      id_grp_payment_type,
      'Оплата мероприятий (выручка)' AS "Метрика",
      "Оплата мероприятий (выручка)"::double precision AS "Значение",
      1.52 AS "Порядок"
    FROM
      source
    WHERE
      "Оплата мероприятий (выручка)" IS NOT NULL
    UNION ALL
    SELECT
      id_club,
      --id_user,
      payment_time,
      payment_time_local,
      id_payment_type,
      id_grp_payment_type,
      'Оплата других доходов (выручка)' AS "Метрика",
      "Оплата других доходов (выручка)"::double precision AS "Значение",
      1.53 AS "Порядок"
    FROM
      source
    WHERE
      "Оплата других доходов (выручка)" IS NOT NULL
    UNION ALL
    SELECT
      id_club,
      --id_user,
      payment_time,
      payment_time_local,
      id_payment_type,
      id_grp_payment_type,
      'Прочая выручка' AS "Метрика",
      COALESCE(
        "Продажи подарочных карт (выручка)"::double precision,
        0
      ) + COALESCE(
        "Оплата мероприятий (выручка)"::double precision,
        0
      ) + COALESCE(
        "Оплата других доходов (выручка)"::double precision,
        0
      ) AS "Значение",
      1.5 AS "Порядок"
    FROM
      source
    WHERE
      COALESCE(
        "Продажи подарочных карт (выручка)"::double precision,
        0
      ) + COALESCE(
        "Оплата мероприятий (выручка)"::double precision,
        0
      ) + COALESCE(
        "Оплата других доходов (выручка)"::double precision,
        0
      ) != 0
    UNION ALL
    SELECT
      id_club,
      --id_user,
      payment_time,
      payment_time_local,
      id_payment_type,
      id_grp_payment_type,
      'Выручка итого' AS "Метрика",
      COALESCE("Наличка"::double precision, 0) + COALESCE("Безнал"::double precision, 0) + COALESCE("Онлайн (APP)"::double precision, 0) + COALESCE(
        "Продажи подарочных карт (выручка)"::double precision,
        0
      ) + COALESCE(
        "Оплата мероприятий (выручка)"::double precision,
        0
      ) + COALESCE(
        "Оплата других доходов (выручка)"::double precision,
        0
      ) AS "Значение",
      1.1 AS "Порядок"
    FROM
      source
    WHERE
      COALESCE("Наличка"::double precision, 0) + COALESCE("Безнал"::double precision, 0) + COALESCE("Онлайн (APP)"::double precision, 0) + COALESCE(
        "Продажи подарочных карт (выручка)"::double precision,
        0
      ) + COALESCE(
        "Оплата мероприятий (выручка)"::double precision,
        0
      ) + COALESCE(
        "Оплата других доходов (выручка)"::double precision,
        0
      ) != 0
    UNION ALL
    SELECT
      id_club,
      --id_user,
      payment_time,
      payment_time_local,
      id_payment_type,
      id_grp_payment_type,
      'Бонусы команде (расход)' AS "Метрика",
      "Бонусы команде (расход)"::double precision AS "Значение",
      2.15 AS "Порядок"
    FROM
      source
    WHERE
      "Бонусы команде (расход)" IS NOT NULL
    UNION ALL
    SELECT
      id_club,
      --id_user,
      payment_time,
      payment_time_local,
      id_payment_type,
      id_grp_payment_type,
      'Зарплата менеджеров (расход)' AS "Метрика",
      "Зарплата менеджеров (расход)"::double precision AS "Значение",
      2.12 AS "Порядок"
    FROM
      source
    WHERE
      "Зарплата менеджеров (расход)" IS NOT NULL
    UNION ALL
    SELECT
      id_club,
      --id_user,
      payment_time,
      payment_time_local,
      id_payment_type,
      id_grp_payment_type,
      'Зарплата сотрудников (расход)' AS "Метрика",
      "Зарплата сотрудников (расход)"::double precision AS "Значение",
      2.11 AS "Порядок"
    FROM
      source
    WHERE
      "Зарплата сотрудников (расход)" IS NOT NULL
    UNION ALL
    SELECT
      id_club,
      --id_user,
      payment_time,
      payment_time_local,
      id_payment_type,
      id_grp_payment_type,
      'Интернет (расход)' AS "Метрика",
      "Интернет (расход)"::double precision AS "Значение",
      2.14 AS "Порядок"
    FROM
      source
    WHERE
      "Интернет (расход)" IS NOT NULL
    UNION ALL
    SELECT
      id_club,
      --id_user,
      payment_time,
      payment_time_local,
      id_payment_type,
      id_grp_payment_type,
      'Коммунальные услуги (расход)' AS "Метрика",
      "Коммунальные услуги (расход)"::double precision AS "Значение",
      2.13 AS "Порядок"
    FROM
      source
    WHERE
      "Коммунальные услуги (расход)" IS NOT NULL
    UNION ALL
    SELECT
      id_club,
      --id_user,
      payment_time,
      payment_time_local,
      id_payment_type,
      id_grp_payment_type,
      'Маркетинг и реклама (расход)' AS "Метрика",
      "Маркетинг и реклама (расход)"::double precision AS "Значение",
      2.16 AS "Порядок"
    FROM
      source
    WHERE
      "Маркетинг и реклама (расход)" IS NOT NULL
    UNION ALL
    SELECT
      id_club,
      --id_user,
      payment_time,
      payment_time_local,
      id_payment_type,
      id_grp_payment_type,
      'Расходы бара (расход)' AS "Метрика",
      "Расходы бара (расход)"::double precision AS "Значение",
      2.17 AS "Порядок"
    FROM
      source
    WHERE
      "Расходы бара (расход)" IS NOT NULL
    UNION ALL
    SELECT
      id_club,
      --id_user,
      payment_time,
      payment_time_local,
      id_payment_type,
      id_grp_payment_type,
      'Уборка (расход)' AS "Метрика",
      "Уборка (расход)"::double precision AS "Значение",
      2.18 AS "Порядок"
    FROM
      source
    WHERE
      "Уборка (расход)" IS NOT NULL
    UNION ALL
    SELECT
      id_club,
      --id_user,
      payment_time,
      payment_time_local,
      id_payment_type,
      id_grp_payment_type,
      'Оплата CRM и цифровых платежей (расход)' AS "Метрика",
      "Оплата CRM и цифровых платежей (расход)"::double precision AS "Значение",
      2.19 AS "Порядок"
    FROM
      source
    WHERE
      "Оплата CRM и цифровых платежей (расход)" IS NOT NULL
    UNION ALL
    SELECT
      id_club,
      --id_user,
      payment_time,
      payment_time_local,
      id_payment_type,
      id_grp_payment_type,
      'Банковские услуги (расход)' AS "Метрика",
      "Банковские услуги (расход)"::double precision AS "Значение",
      2.191 AS "Порядок"
    FROM
      source
    WHERE
      "Банковские услуги (расход)" IS NOT NULL
    UNION ALL
    SELECT
      id_club,
      --id_user,
      payment_time,
      payment_time_local,
      id_payment_type,
      id_grp_payment_type,
      'Бухгалтерские услуги (расход)' AS "Метрика",
      "Бухгалтерские услуги (расход)"::double precision AS "Значение",
      2.192 AS "Порядок"
    FROM
      source
    WHERE
      "Бухгалтерские услуги (расход)" IS NOT NULL
    UNION ALL
    SELECT
      id_club,
      --id_user,
      payment_time,
      payment_time_local,
      id_payment_type,
      id_grp_payment_type,
      'Оборудование (расход)' AS "Метрика",
      "Оборудование (расход)"::double precision AS "Значение",
      2.193 AS "Порядок"
    FROM
      source
    WHERE
      "Оборудование (расход)" IS NOT NULL
    UNION ALL
    SELECT
      id_club,
      --id_user,
      payment_time,
      payment_time_local,
      id_payment_type,
      id_grp_payment_type,
      'Ремонт (расход)' AS "Метрика",
      "Ремонт (расход)"::double precision AS "Значение",
      2.194 AS "Порядок"
    FROM
      source
    WHERE
      "Ремонт (расход)" IS NOT NULL
    UNION ALL
    SELECT
      id_club,
      --id_user,
      payment_time,
      payment_time_local,
      id_payment_type,
      id_grp_payment_type,
      'Бытовые расходы (расход)' AS "Метрика",
      "Бытовые расходы (расход)"::double precision AS "Значение",
      2.195 AS "Порядок"
    FROM
      source
    WHERE
      "Бытовые расходы (расход)" IS NOT NULL
    UNION ALL
    SELECT
      id_club,
      --id_user,
      payment_time,
      payment_time_local,
      id_payment_type,
      id_grp_payment_type,
      'Призовой фонд турниров (расход)' AS "Метрика",
      "Призовой фонд турниров (расход)"::double precision AS "Значение",
      2.196 AS "Порядок"
    FROM
      source
    WHERE
      "Призовой фонд турниров (расход)" IS NOT NULL
    UNION ALL
    SELECT
      id_club,
      --id_user,
      payment_time,
      payment_time_local,
      id_payment_type,
      id_grp_payment_type,
      'Транспортные расходы (расход)' AS "Метрика",
      "Транспортные расходы (расход)"::double precision AS "Значение",
      2.197 AS "Порядок"
    FROM
      source
    WHERE
      "Транспортные расходы (расход)" IS NOT NULL
    UNION ALL
    SELECT
      id_club,
      --id_user,
      payment_time,
      payment_time_local,
      id_payment_type,
      id_grp_payment_type,
      'Аренда (расход)' AS "Метрика",
      "Аренда (расход)"::double precision AS "Значение",
      2.198 AS "Порядок"
    FROM
      source
    WHERE
      "Аренда (расход)" IS NOT NULL
    UNION ALL
    SELECT
      id_club,
      --id_user,
      payment_time,
      payment_time_local,
      id_payment_type,
      id_grp_payment_type,
      'Юридические расходы (расход)' AS "Метрика",
      "Юридические расходы (расход)"::double precision AS "Значение",
      2.199 AS "Порядок"
    FROM
      source
    WHERE
      "Юридические расходы (расход)" IS NOT NULL
    UNION ALL
    SELECT
      id_club,
      --id_user,
      payment_time,
      payment_time_local,
      id_payment_type,
      id_grp_payment_type,
      'Прочие расходы (расход)' AS "Метрика",
      "Прочие расходы (расход)"::double precision AS "Значение",
      2.1991 AS "Порядок"
    FROM
      source
    WHERE
      "Прочие расходы (расход)" IS NOT NULL
    UNION ALL
    SELECT
      id_club,
      --id_user,
      payment_time,
      payment_time_local,
      id_payment_type,
      id_grp_payment_type,
      'Другие расходов (расход)' AS "Метрика",
      "Другие расходов (расход)"::double precision AS "Значение",
      2.1992 AS "Порядок"
    FROM
      source
    WHERE
      "Другие расходов (расход)" IS NOT NULL
    UNION ALL
    SELECT
      id_club,
      --id_user,
      payment_time,
      payment_time_local,
      id_payment_type,
      id_grp_payment_type,
      'НДС (расход)' AS "Метрика",
      "НДС (расход)"::double precision AS "Значение",
      2.1993 AS "Порядок"
    FROM
      source
    WHERE
      "НДС (расход)" IS NOT NULL
    UNION ALL
    SELECT
      id_club,
      --id_user,
      payment_time,
      payment_time_local,
      id_payment_type,
      id_grp_payment_type,
      'Налоги и сборы (расход)' AS "Метрика",
      "Налоги и сборы (расход)"::double precision AS "Значение",
      2.1994 AS "Порядок"
    FROM
      source
    WHERE
      "Налоги и сборы (расход)" IS NOT NULL
    UNION ALL
    SELECT
      id_club,
      --id_user,
      payment_time,
      payment_time_local,
      id_payment_type,
      id_grp_payment_type,
      'Оплата мероприятий наличными (расход)' AS "Метрика",
      "Оплата мероприятий наличными (расход)"::double precision AS "Значение",
      2.1995 AS "Порядок"
    FROM
      source
    WHERE
      "Оплата мероприятий наличными (расход)" IS NOT NULL
    UNION ALL
    SELECT
      id_club,
      --id_user,
      payment_time,
      payment_time_local,
      id_payment_type,
      id_grp_payment_type,
      'Оплата других доходов (расход)' AS "Метрика",
      "Оплата других доходов (расход)"::double precision AS "Значение",
      2.1996 AS "Порядок"
    FROM
      source
    WHERE
      "Оплата других доходов (расход)" IS NOT NULL
    UNION ALL
    SELECT
      id_club,
      --id_user,
      payment_time,
      payment_time_local,
      id_payment_type,
      id_grp_payment_type,
      'Расходы итого' AS "Метрика",
      COALESCE("Бонусы команде (расход)"::double precision, 0) + COALESCE(
        "Зарплата менеджеров (расход)"::double precision,
        0
      ) + COALESCE(
        "Зарплата сотрудников (расход)"::double precision,
        0
      ) + COALESCE("Интернет (расход)"::double precision, 0) + COALESCE(
        "Коммунальные услуги (расход)"::double precision,
        0
      ) + COALESCE(
        "Маркетинг и реклама (расход)"::double precision,
        0
      ) + COALESCE("Расходы бара (расход)"::double precision, 0) + COALESCE("Уборка (расход)"::double precision, 0) + COALESCE(
        "Оплата CRM и цифровых платежей (расход)"::double precision,
        0
      ) + COALESCE("Банковские услуги (расход)"::double precision, 0) + COALESCE(
        "Бухгалтерские услуги (расход)"::double precision,
        0
      ) + COALESCE("Оборудование (расход)"::double precision, 0) + COALESCE("Ремонт (расход)"::double precision, 0) + COALESCE("Бытовые расходы (расход)"::double precision, 0) + COALESCE(
        "Призовой фонд турниров (расход)"::double precision,
        0
      ) + COALESCE(
        "Транспортные расходы (расход)"::double precision,
        0
      ) + COALESCE("Аренда (расход)"::double precision, 0) + COALESCE(
        "Юридические расходы (расход)"::double precision,
        0
      ) + COALESCE("Прочие расходы (расход)"::double precision, 0) + COALESCE("Другие расходов (расход)"::double precision, 0) + COALESCE("НДС (расход)"::double precision, 0) + COALESCE("Налоги и сборы (расход)"::double precision, 0) + COALESCE(
        "Оплата мероприятий наличными (расход)"::double precision,
        0
      ) + COALESCE(
        "Оплата других доходов (расход)"::double precision,
        0
      ) AS "Значение",
      2.1 AS "Порядок"
    FROM
      source
    WHERE
      COALESCE("Бонусы команде (расход)"::double precision, 0) + COALESCE(
        "Зарплата менеджеров (расход)"::double precision,
        0
      ) + COALESCE(
        "Зарплата сотрудников (расход)"::double precision,
        0
      ) + COALESCE("Интернет (расход)"::double precision, 0) + COALESCE(
        "Коммунальные услуги (расход)"::double precision,
        0
      ) + COALESCE(
        "Маркетинг и реклама (расход)"::double precision,
        0
      ) + COALESCE("Расходы бара (расход)"::double precision, 0) + COALESCE("Уборка (расход)"::double precision, 0) + COALESCE(
        "Оплата CRM и цифровых платежей (расход)"::double precision,
        0
      ) + COALESCE("Банковские услуги (расход)"::double precision, 0) + COALESCE(
        "Бухгалтерские услуги (расход)"::double precision,
        0
      ) + COALESCE("Оборудование (расход)"::double precision, 0) + COALESCE("Ремонт (расход)"::double precision, 0) + COALESCE("Бытовые расходы (расход)"::double precision, 0) + COALESCE(
        "Призовой фонд турниров (расход)"::double precision,
        0
      ) + COALESCE(
        "Транспортные расходы (расход)"::double precision,
        0
      ) + COALESCE("Аренда (расход)"::double precision, 0) + COALESCE(
        "Юридические расходы (расход)"::double precision,
        0
      ) + COALESCE("Прочие расходы (расход)"::double precision, 0) + COALESCE("Другие расходов (расход)"::double precision, 0) + COALESCE("НДС (расход)"::double precision, 0) + COALESCE("Налоги и сборы (расход)"::double precision, 0) + COALESCE(
        "Оплата мероприятий наличными (расход)"::double precision,
        0
      ) + COALESCE(
        "Оплата других доходов (расход)"::double precision,
        0
      ) != 0
    UNION ALL
    SELECT
      id_club,
      --id_user,
      payment_time,
      payment_time_local,
      id_payment_type,
      id_grp_payment_type,
      'Прибыль' AS "Метрика",
      (
        (
          COALESCE("Наличка"::double precision, 0) + COALESCE("Безнал"::double precision, 0) + COALESCE("Онлайн (APP)"::double precision, 0) + COALESCE(
            "Продажи подарочных карт (выручка)"::double precision,
            0
          ) + COALESCE(
            "Оплата мероприятий (выручка)"::double precision,
            0
          ) + COALESCE(
            "Оплата других доходов (выручка)"::double precision,
            0
          )
        ) - (
          COALESCE("Бонусы команде (расход)"::double precision, 0) + COALESCE(
            "Зарплата менеджеров (расход)"::double precision,
            0
          ) + COALESCE(
            "Зарплата сотрудников (расход)"::double precision,
            0
          ) + COALESCE("Интернет (расход)"::double precision, 0) + COALESCE(
            "Коммунальные услуги (расход)"::double precision,
            0
          ) + COALESCE(
            "Маркетинг и реклама (расход)"::double precision,
            0
          ) + COALESCE("Расходы бара (расход)"::double precision, 0) + COALESCE("Уборка (расход)"::double precision, 0) + COALESCE(
            "Оплата CRM и цифровых платежей (расход)"::double precision,
            0
          ) + COALESCE("Банковские услуги (расход)"::double precision, 0) + COALESCE(
            "Бухгалтерские услуги (расход)"::double precision,
            0
          ) + COALESCE("Оборудование (расход)"::double precision, 0) + COALESCE("Ремонт (расход)"::double precision, 0) + COALESCE("Бытовые расходы (расход)"::double precision, 0) + COALESCE(
            "Призовой фонд турниров (расход)"::double precision,
            0
          ) + COALESCE(
            "Транспортные расходы (расход)"::double precision,
            0
          ) + COALESCE("Аренда (расход)"::double precision, 0) + COALESCE(
            "Юридические расходы (расход)"::double precision,
            0
          ) + COALESCE("Прочие расходы (расход)"::double precision, 0) + COALESCE("Другие расходов (расход)"::double precision, 0) + COALESCE("НДС (расход)"::double precision, 0) + COALESCE("Налоги и сборы (расход)"::double precision, 0) + COALESCE(
            "Оплата мероприятий наличными (расход)"::double precision,
            0
          ) + COALESCE(
            "Оплата других доходов (расход)"::double precision,
            0
          )
        )
      ) AS "Значение",
      3.1 AS "Порядок"
    FROM
      source
    WHERE
      (
        (
          COALESCE("Наличка"::double precision, 0) + COALESCE("Безнал"::double precision, 0) + COALESCE("Онлайн (APP)"::double precision, 0) + COALESCE(
            "Продажи подарочных карт (выручка)"::double precision,
            0
          ) + COALESCE(
            "Оплата мероприятий (выручка)"::double precision,
            0
          ) + COALESCE(
            "Оплата других доходов (выручка)"::double precision,
            0
          )
        ) - (
          COALESCE("Бонусы команде (расход)"::double precision, 0) + COALESCE(
            "Зарплата менеджеров (расход)"::double precision,
            0
          ) + COALESCE(
            "Зарплата сотрудников (расход)"::double precision,
            0
          ) + COALESCE("Интернет (расход)"::double precision, 0) + COALESCE(
            "Коммунальные услуги (расход)"::double precision,
            0
          ) + COALESCE(
            "Маркетинг и реклама (расход)"::double precision,
            0
          ) + COALESCE("Расходы бара (расход)"::double precision, 0) + COALESCE("Уборка (расход)"::double precision, 0) + COALESCE(
            "Оплата CRM и цифровых платежей (расход)"::double precision,
            0
          ) + COALESCE("Банковские услуги (расход)"::double precision, 0) + COALESCE(
            "Бухгалтерские услуги (расход)"::double precision,
            0
          ) + COALESCE("Оборудование (расход)"::double precision, 0) + COALESCE("Ремонт (расход)"::double precision, 0) + COALESCE("Бытовые расходы (расход)"::double precision, 0) + COALESCE(
            "Призовой фонд турниров (расход)"::double precision,
            0
          ) + COALESCE(
            "Транспортные расходы (расход)"::double precision,
            0
          ) + COALESCE("Аренда (расход)"::double precision, 0) + COALESCE(
            "Юридические расходы (расход)"::double precision,
            0
          ) + COALESCE("Прочие расходы (расход)"::double precision, 0) + COALESCE("Другие расходов (расход)"::double precision, 0) + COALESCE("НДС (расход)"::double precision, 0) + COALESCE("Налоги и сборы (расход)"::double precision, 0) + COALESCE(
            "Оплата мероприятий наличными (расход)"::double precision,
            0
          ) + COALESCE(
            "Оплата других доходов (расход)"::double precision,
            0
          )
        )
      ) != 0