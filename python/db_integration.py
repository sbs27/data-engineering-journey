# python/db_integration.py
import sqlite3
import pandas as pd

def demonstrate_cte_in_python():
    """Show how CTEs work in Python with SQLite"""
    print("🔗 INTEGRATING SQL CTEs WITH PYTHON")
    print("=" * 50)
    
    # Create in-memory database
    conn = sqlite3.connect(':memory:')
    
    # Create and populate employees table
    employees_data = [
        (1, 'John CEO', 'Executive', 100000, None),
        (2, 'Alice VP', 'Engineering', 80000, 1),
        (3, 'Bob Manager', 'Engineering', 70000, 2),
        (4, 'Charlie Dev', 'Engineering', 60000, 3),
        (5, 'Diana VP', 'Sales', 85000, 1)
    ]
    
    conn.execute('''
        CREATE TABLE employees (
            id INTEGER PRIMARY KEY,
            name TEXT,
            department TEXT,
            salary REAL,
            manager_id INTEGER
        )
    ''')
    conn.executemany('INSERT INTO employees VALUES (?, ?, ?, ?, ?)', employees_data)
    conn.commit()
    
    # Execute CTE query using pandas
    print("\n EXECUTING CTE QUERY WITH PANDAS:")
    
    cte_query = """
    WITH department_stats AS (
        SELECT 
            department,
            AVG(salary) as avg_salary,
            COUNT(*) as employee_count
        FROM employees
        GROUP BY department
    )
    SELECT * FROM department_stats
    """
    
    result_df = pd.read_sql_query(cte_query, conn)
    print(result_df)
    
    # Combine generator with database operations
    print("\n COMBINING GENERATORS WITH DATABASE OPERATIONS:")
    
    def process_employees_generator():
        """Generator that processes employee data"""
        cursor = conn.execute("SELECT * FROM employees")
        for row in cursor:
            employee_id, name, department, salary, manager_id = row
            # Simulate processing
            processed_data = {
                'id': employee_id,
                'name': name.upper(),
                'department': department,
                'annual_salary': salary,
                'monthly_salary': round(salary / 12, 2)
            }
            yield processed_data
    
    print("Processed employee data:")
    for i, employee in enumerate(process_employees_generator()):
        if i < 3:  # Show first 3
            print(f"   {employee}")
    
    conn.close()
    print("\n INTEGRATION COMPLETED!")

if __name__ == "__main__":
    demonstrate_cte_in_python()