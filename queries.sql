--Получает список всех компаний и количество вакансий у каждой компании
SELECT * FROM companies

--получает список всех вакансий с указанием названия компании, названия вакансии и зарплаты и ссылки на вакансию
SELECT company_name, vacancy_name, salary_from, salary_to, vacancy_url
FROM vacancies

--получает среднюю зарплату по вакансиям
SELECT company_name, vacancy_name, salary_from, salary_to, CASE
           WHEN salary_from IS NOT NULL AND salary_to IS NULL THEN salary_from
           WHEN salary_from IS NULL AND salary_to IS NOT NULL THEN salary_to
           WHEN salary_from IS NOT NULL AND salary_to IS NOT NULL THEN (salary_from + salary_to) / 2
           ELSE NULL
        END as avg_salary, vacancy_url
FROM vacancies

--получает список всех вакансий, у которых зарплата выше средней по всем вакансиям
WITH salaries AS (
    SELECT company_name, vacancy_name, salary_from, salary_to, vacancy_url,
       CASE
           WHEN salary_from IS NOT NULL AND salary_to IS NULL THEN salary_from
           WHEN salary_from IS NULL AND salary_to IS NOT NULL THEN salary_to
           WHEN salary_from IS NOT NULL AND salary_to IS NOT NULL THEN (salary_from + salary_to) / 2
           ELSE NULL
        END as avg_salary
FROM vacancies
)
SELECT company_name, vacancy_name, salary_from, salary_to, vacancy_url
FROM salaries
WHERE avg_salary > (SELECT AVG(avg_salary) FROM salaries)

--получает список всех вакансий, в названии которых содержатся переданные в метод слова, например “python”
SELECT company_name, vacancy_name, salary_from, salary_to, vacancy_url
FROM vacancies
WHERE lower(vacancy_name) LIKE lower(%s)

--Пример использования ключевого слова
search_keyword = f"%{keyword}%"