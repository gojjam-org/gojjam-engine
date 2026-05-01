SELECT 
    company_name,
    COUNT(id) as employee_count
FROM sample_users
GROUP BY 1