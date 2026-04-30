--1. Определить регионы с наибольшим количеством зарегистрированных доноров.
WITH t1 AS (
	SELECT 
		uad.region,
		COUNT(uad.id) OVER (PARTITION BY uad.region) cnt_donors_reg,
		COUNT(uad.id) OVER (PARTITION BY NULL) cnt_donors_all
	FROM donorsearch.user_anon_data uad 
)
SELECT DISTINCT region,
	cnt_donors_reg,
	ROUND(cnt_donors_reg::numeric / cnt_donors_all * 100, 2) prcnt_donors_reg
FROM t1
ORDER BY cnt_donors_reg DESC
LIMIT 10
;


--2. Изучить динамику общего количества донаций в месяц за 2022 и 2023 годы.
SELECT 
	EXTRACT(MONTH FROM dan.donation_date) months,
	SUM(CASE WHEN EXTRACT(YEAR FROM dan.donation_date) = '2022' THEN 1 ELSE 0 END) "2022",
	SUM(CASE WHEN EXTRACT(YEAR FROM dan.donation_date) = '2023' THEN 1 ELSE 0 END) "2023"
FROM donorsearch.donation_anon dan
WHERE (EXTRACT(YEAR FROM dan.donation_date) = '2022' OR EXTRACT(YEAR FROM dan.donation_date) = '2023')
GROUP BY months
;


--3. Определить наиболее активных доноров в системе, учитывая только данные о зарегистрированных и подтвержденных донациях.
WITH t1 AS (
SELECT DISTINCT
	user_id,
	EXTRACT(YEAR FROM AGE(MAX(coalesce(donation_date, plan_date)), MIN(coalesce(donation_date, plan_date)))) donation_years,
	COUNT(donation_date) + COUNT(plan_date) FILTER (WHERE plan_date > donation_date) cnt_donations_all
FROM donorsearch.donation_anon
GROUP BY user_id
ORDER BY cnt_donations_all DESC
)
SELECT user_id,
	donation_years,
	cnt_donations_all,
	ROUND(cnt_donations_all/ donation_years, 2) donations_per_year
FROM t1
ORDER BY cnt_donations_all DESC, donations_per_year
LIMIT 10
;


--4. Оценить, как система бонусов влияет на зарегистрированные в системе донации.
SELECT 
	DISTINCT partner || ': ' || bonus_name AS bonuss,
	COUNT(DISTINCT user_id) cnt_donors
FROM donorsearch.user_anon_bonus uab
GROUP BY bonuss
ORDER BY cnt_donors DESC, bonuss
LIMIT 20
;


--5. Исследовать вовлечение новых доноров через социальные сети. Узнать, сколько по каким каналам пришло доноров, и среднее количество донаций по каждому каналу.
SELECT 
	'vk' AS channel, 
	COUNT(DISTINCT uad.id) cnt_donors,
	ROUND(AVG(dan.id), 2) avg_donations
FROM donorsearch.user_anon_data uad 
LEFT JOIN donorsearch.donation_anon dan ON uad.id = dan.user_id
WHERE uad.autho_vk IS TRUE
	UNION
SELECT 
	'yandex' AS channel, 
	COUNT(DISTINCT uad.id) cnt_donors,
	ROUND(AVG(dan.id), 2) avg_donations
FROM donorsearch.user_anon_data uad 
LEFT JOIN donorsearch.donation_anon dan ON uad.id = dan.user_id
WHERE uad.autho_yandex IS TRUE
	UNION
SELECT 
	'tg' AS channel, 
	COUNT(DISTINCT uad.id) cnt_donors,
	ROUND(AVG(dan.id), 2) avg_donations
FROM donorsearch.user_anon_data uad 
LEFT JOIN donorsearch.donation_anon dan ON uad.id = dan.user_id
WHERE uad.autho_tg IS TRUE
	UNION
SELECT 
	'google' AS channel, 
	COUNT(DISTINCT uad.id) cnt_donors,
	ROUND(AVG(dan.id), 2) avg_donations
FROM donorsearch.user_anon_data uad 
LEFT JOIN donorsearch.donation_anon dan ON uad.id = dan.user_id
WHERE uad.autho_google IS TRUE
	UNION
SELECT 
	'ok' AS channel, 
	COUNT(DISTINCT uad.id) cnt_donors,
	ROUND(AVG(dan.id), 2) avg_donations
FROM donorsearch.user_anon_data uad 
LEFT JOIN donorsearch.donation_anon dan ON uad.id = dan.user_id
WHERE uad.autho_ok IS TRUE
ORDER BY cnt_donors DESC, avg_donations DESC
;


--6. Сравнить активность однократных доноров со средней активностью повторных доноров.
WITH t1 AS (
SELECT DISTINCT dan.user_id,
	COUNT(dan.id) OVER (PARTITION BY dan.user_id) cnt_donations_user,
	COUNT(dan.id) OVER (PARTITION BY NULL) cnt_donations_all
FROM donorsearch.user_anon_data uad 
JOIN donorsearch.donation_anon dan ON uad.id = dan.user_id
)
SELECT 
	AVG(cnt_donations_user) FILTER (WHERE cnt_donations_user = 1) avg_donations_1,
	ROUND(AVG(cnt_donations_user) FILTER (WHERE cnt_donations_user > 1), 2) avg_donations_few,
	COUNT(DISTINCT user_id) FILTER (WHERE cnt_donations_user = 1) cnt_donors_1,
	COUNT(DISTINCT user_id) FILTER (WHERE cnt_donations_user > 1) cnt_donors_few,
	ROUND((COUNT(DISTINCT user_id) FILTER (WHERE cnt_donations_user > 1))::numeric / COUNT(DISTINCT user_id) FILTER (WHERE cnt_donations_user = 1), 3) ratio_donors,
	ROUND(SUM(cnt_donations_user) FILTER (WHERE cnt_donations_user > 1) / MAX(cnt_donations_all)::NUMERIC, 3) ratio_donations_few
FROM t1
;


--7. Сравнить данные о планируемых донациях с фактическими данными, чтобы оценить эффективность планирования.
 --Всего:
SELECT 
	COUNT(donation_date) cnt_donation_fact,
	COUNT(plan_date) cnt_donation_plan,
	ROUND(COUNT(plan_date)::numeric / COUNT(donation_date) * 100, 2) percent_plan_fact
FROM donorsearch.donation_anon
;

--2022-2023:
WITH t1 AS (
SELECT 
	DATE_TRUNC('MONTH', donation_date) donation_month_fact,
	COUNT(donation_date) cnt_donation_fact
FROM donorsearch.donation_anon
WHERE EXTRACT(YEAR FROM donation_date) BETWEEN '2022' AND '2023'
GROUP BY donation_month_fact
),
t2 AS (
SELECT 
	DATE_TRUNC('MONTH', plan_date) donation_month_plan,
	COUNT(plan_date) cnt_donation_plan
FROM donorsearch.donation_anon
WHERE EXTRACT(YEAR FROM plan_date) BETWEEN '2022' AND '2023'
GROUP BY donation_month_plan
)
SELECT 
	t1.donation_month_fact::date AS months,
	t2.cnt_donation_plan,
	t1.cnt_donation_fact,
	ROUND(cnt_donation_plan::numeric / cnt_donation_fact * 100, 2) percent_plan_fact
FROM t1 
LEFT JOIN t2 ON t1.donation_month_fact = t2.donation_month_plan
;

