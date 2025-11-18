-- Day 2: Window Functions Mastery
-- 1. Salary Ranking with All Three Functions
SELECT name, department, salary,
    ROW_NUMBER() OVER (PARTITION BY department ORDER BY salary DESC) as row_num,
    RANK() OVER (PARTITION BY department ORDER BY salary DESC) as rank,
    DENSE_RANK() OVER (PARTITION BY department ORDER BY salary DESC) as dense_rank
FROM employees;

-- 2. Deduplication Pattern
WITH ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY key_columns ORDER BY timestamp DESC) as rn
    FROM table
)
SELECT * FROM ranked WHERE rn = 1;

-- 3. Complex Multi-criteria Ranking
SELECT name, department, salary, performance_score,
    ROW_NUMBER() OVER (PARTITION BY department ORDER BY salary DESC, performance_score DESC) as overall_rank
FROM employees;
