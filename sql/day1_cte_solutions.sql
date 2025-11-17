-- Day 1: SQL CTE Solutions
-- 1. Employee Hierarchy with Recursive CTE
WITH RECURSIVE employee_hierarchy AS (
    SELECT id, name, manager_id, 1 as level
    FROM employees WHERE manager_id IS NULL
    UNION ALL
    SELECT e.id, e.name, e.manager_id, eh.level + 1
    FROM employees e JOIN employee_hierarchy eh ON e.manager_id = eh.id
)
SELECT * FROM employee_hierarchy ORDER BY level, id;

-- 2. Department Salary Analysis with Multiple CTEs
WITH department_summary AS (
    SELECT department, AVG(salary) as avg_salary, COUNT(*) as count
    FROM employees GROUP BY department
)
SELECT * FROM department_summary;

-- 3. Employees Earning More Than Managers
WITH employee_managers AS (
    SELECT e.name as emp, e.salary as emp_sal, m.name as mgr, m.salary as mgr_sal
    FROM employees e JOIN employees m ON e.manager_id = m.id
)
SELECT * FROM employee_managers WHERE emp_sal > mgr_sal;
