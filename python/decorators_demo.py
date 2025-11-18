"""
DATA ENGINEERING - PYTHON DECORATORS MASTERY
Day 2: Building professional decorators for data engineering workflows
"""

import time
import functools
from datetime import datetime


def timing_decorator(func):
    """
    PERFORMANCE MONITORING DECORATOR
    Measures how long a function takes to execute
    Essential for Data Engineering to optimize slow ETL processes
    """
    @functools.wraps(func)  # Preserves original function's metadata
    def wrapper(*args, **kwargs):
        start_time = time.time()
        print(f"TIMING: Starting {func.__name__} at {datetime.now().strftime('%H:%M:%S')}")
        
        # Execute the original function
        result = func(*args, **kwargs)
        
        end_time = time.time()
        execution_time = end_time - start_time
        
        print(f"TIMING: {func.__name__} completed in {execution_time:.4f} seconds")
        return result
    
    return wrapper


def debug_decorator(func):
    """
    DEBUGGING DECORATOR
    Logs function calls, arguments, and return values
    Great for understanding data flow in complex pipelines
    """
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        print(f"DEBUG: Calling {func.__name__}")
        print(f"DEBUG: Arguments: {args}")
        print(f"DEBUG: Keyword arguments: {kwargs}")
        
        result = func(*args, **kwargs)
        
        print(f"DEBUG: Returned: {result}")
        print(f"DEBUG: {func.__name__} finished")
        return result
    
    return wrapper


def cache_decorator(func):
    """
    CACHING DECORATOR
    Stores results of expensive function calls
    Massive performance boost for repeated calculations in data pipelines
    """
    cache = {}  # Dictionary to store cached results
    
    @functools.wraps(func)
    def wrapper(*args):
        # Create a key from function arguments
        key = str(args)
        
        # Check if result is already cached
        if key in cache:
            print(f"CACHE: Cache hit for {func.__name__}{args}")
            return cache[key]
        else:
            print(f"CACHE: Computing {func.__name__}{args} (not in cache)")
            result = func(*args)
            cache[key] = result  # Store in cache
            return result
    
    return wrapper


def data_quality_decorator(func):
    """
    DATA VALIDATION DECORATOR
    Ensures data meets quality standards before processing
    Critical for reliable data engineering pipelines
    """
    @functools.wraps(func)
    def wrapper(data, *args, **kwargs):
        print(f"QUALITY: Running data quality checks for {func.__name__}...")
        
        # Data Quality Checks
        if data is None:
            raise ValueError("QUALITY ERROR: Data cannot be None")
        if isinstance(data, (list, tuple)) and len(data) == 0:
            raise ValueError("QUALITY ERROR: Data cannot be empty")
        if isinstance(data, dict) and len(data) == 0:
            raise ValueError("QUALITY ERROR: Data dictionary cannot be empty")
        
        # Type checking
        if not isinstance(data, (list, tuple, dict)):
            raise ValueError(f"QUALITY ERROR: Expected list/tuple/dict, got {type(data)}")
        
        print("QUALITY: All data quality checks passed!")
        
        # Proceed with original function
        return func(data, *args, **kwargs)
    
    return wrapper


# PRACTICAL DATA ENGINEERING EXAMPLES

@timing_decorator
def process_large_dataset(data_size):
    """
    Simulate processing a large dataset
    Decorator will automatically time this operation
    """
    print(f"PROCESSING: Processing {data_size:,} records...")
    time.sleep(2)  # Simulate processing time
    return f"Processed {data_size:,} records"


@debug_decorator
@timing_decorator  # Multiple decorators stack!
def calculate_department_stats(employees, department):
    """
    Calculate statistics for a department
    Both timing and debug decorators will work together
    """
    dept_employees = [e for e in employees if e['department'] == department]
    avg_salary = sum(e['salary'] for e in dept_employees) / len(dept_employees)
    max_salary = max(e['salary'] for e in dept_employees)
    
    return {
        'department': department,
        'employee_count': len(dept_employees),
        'avg_salary': round(avg_salary, 2),
        'max_salary': max_salary
    }


@cache_decorator
def expensive_data_transformation(n):
    """
    Simulate an expensive calculation that we might want to cache
    Common in feature engineering or data aggregation
    """
    print(f"COMPUTING: Performing expensive calculation for {n}...")
    time.sleep(1)  # Simulate slow computation
    return n * n  # Some expensive result


@data_quality_decorator
@timing_decorator
def analyze_sales_data(sales_data):
    """
    Analyze sales data with automatic quality checks and timing
    """
    total_sales = sum(sales_data)
    avg_sales = total_sales / len(sales_data)
    return {
        'total_sales': total_sales,
        'average_sales': round(avg_sales, 2),
        'data_points': len(sales_data)
    }


def demonstrate_basic_concepts():
    """
    EXPLAIN BASIC DECORATOR CONCEPTS
    """
    print("=" * 70)
    print("PYTHON DECORATORS - BASIC CONCEPTS")
    print("=" * 70)
    
    # 1. Show that functions are first-class objects
    print("\n1. FUNCTIONS AS FIRST-CLASS OBJECTS")
    print("-" * 40)
    
    def hello():
        return "Hello World!"
    
    # Assign function to variable
    my_func = hello
    print(f"Function assigned to variable: {my_func()}")
    
    # Pass function as argument
    def caller(func):
        return func()
    
    print(f"Function passed as argument: {caller(hello)}")
    
    # 2. Show inner functions
    print("\n2. INNER FUNCTIONS")
    print("-" * 40)
    
    def outer_function():
        print("OUTER: Started")
        
        def inner_function():
            print("INNER: Executed")
            return "Inner result"
        
        result = inner_function()
        print("OUTER: Finished")
        return result
    
    print(f"Result: {outer_function()}")
    
    # 3. Show manual decoration
    print("\n3. MANUAL DECORATION PROCESS")
    print("-" * 40)
    
    def simple_decorator(func):
        def wrapper():
            print("DECORATOR: Before function")
            result = func()
            print("DECORATOR: After function")
            return result
        return wrapper
    
    def greet():
        return "Hello!"
    
    # Manual decoration
    decorated_greet = simple_decorator(greet)
    print(f"Manual decoration result: {decorated_greet()}")


def demonstrate_decorators():
    """
    MAIN DEMONSTRATION: Show all decorators in action
    """
    print("\n" + "=" * 70)
    print("DATA ENGINEERING DECORATORS IN ACTION")
    print("=" * 70)
    
    # Sample data for our demonstrations
    sample_employees = [
        {'name': 'Alice', 'department': 'Engineering', 'salary': 80000},
        {'name': 'Bob', 'department': 'Engineering', 'salary': 70000},
        {'name': 'Charlie', 'department': 'Sales', 'salary': 60000},
        {'name': 'Diana', 'department': 'Sales', 'salary': 85000}
    ]
    
    # 1. TIMING DECORATOR DEMO
    print("\n1. TIMING DECORATOR (Performance Monitoring)")
    print("-" * 50)
    process_large_dataset(1000000)
    
    # 2. DEBUG + TIMING DECORATORS (Stacked!)
    print("\n2. DEBUG + TIMING DECORATORS (Stacked)")
    print("-" * 50)
    stats = calculate_department_stats(sample_employees, 'Engineering')
    print(f"Result: {stats}")
    
    # 3. CACHING DECORATOR DEMO
    print("\n3. CACHING DECORATOR (Performance Optimization)")
    print("-" * 50)
    
    # First call - will compute
    result1 = expensive_data_transformation(10)
    print(f"Result: {result1}")
    
    # Second call with same arguments - will use cache!
    result2 = expensive_data_transformation(10)
    print(f"Result: {result2}")
    
    # Different arguments - will compute again
    result3 = expensive_data_transformation(20)
    print(f"Result: {result3}")
    
    # 4. DATA QUALITY DECORATOR
    print("\n4. DATA QUALITY DECORATOR (Validation)")
    print("-" * 50)
    
    # Test with good data
    good_sales = [100, 200, 150, 300]
    result = analyze_sales_data(good_sales)
    print(f"Sales Analysis: {result}")
    
    # Test with bad data (commented out to avoid crash)
    print("\n5. ERROR HANDLING DEMO (Commented out)")
    print("-" * 50)
    # Uncomment to see error handling:
    # analyze_sales_data([])  # This would raise a quality error
    
    print("\n" + "=" * 70)
    print("DATA ENGINEERING APPLICATIONS:")
    print("• Timing: Monitor ETL pipeline performance")
    print("• Debugging: Trace data flow in complex systems") 
    print("• Caching: Speed up repeated data transformations")
    print("• Data Quality: Validate inputs automatically")
    print("• Stacking: Combine multiple behaviors")
    print("=" * 70)


def advanced_decorator_with_parameters():
    """
    ADVANCED: Decorators that accept their own parameters
    """
    print("\n" + "=" * 70)
    print("ADVANCED: PARAMETERIZED DECORATORS")
    print("=" * 70)
    
    def retry_decorator(max_retries=3, delay=1):
        """
        Decorator that retries failed operations
        Useful for unreliable data sources or network calls
        """
        def decorator(func):
            @functools.wraps(func)
            def wrapper(*args, **kwargs):
                for attempt in range(max_retries):
                    try:
                        print(f"RETRY: Attempt {attempt + 1} of {max_retries}")
                        return func(*args, **kwargs)
                    except Exception as e:
                        print(f"RETRY: Attempt {attempt + 1} failed: {e}")
                        if attempt < max_retries - 1:
                            print(f"RETRY: Waiting {delay} seconds before retry...")
                            time.sleep(delay)
                        else:
                            print("RETRY: All attempts failed!")
                            raise
            return wrapper
        return decorator
    
    @retry_decorator(max_retries=2, delay=0.5)
    def unreliable_data_fetch(should_fail=True):
        """
        Simulate an unreliable data source that might fail
        """
        if should_fail:
            raise ConnectionError("Database connection failed!")
        return "Data fetched successfully"
    
    print("Testing retry decorator with failing function:")
    try:
        unreliable_data_fetch(should_fail=True)
    except Exception as e:
        print(f"Expected error caught: {e}")
    
    print("\nTesting retry decorator with successful function:")
    result = unreliable_data_fetch(should_fail=False)
    print(f"Result: {result}")


if __name__ == "__main__":
    # Run all demonstrations
    demonstrate_basic_concepts()
    demonstrate_decorators()
    advanced_decorator_with_parameters()
    
    print("\n" + "=" * 70)
    print("QUICK REFERENCE: Decorator Syntax Patterns")
    print("=" * 70)
    print("""
# Basic decorator pattern:
def my_decorator(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        # Before function
        result = func(*args, **kwargs)
        # After function  
        return result
    return wrapper

# Using decorator:
@my_decorator
def my_function():
    pass

# Multiple decorators (bottom to top execution):
@decorator3
@decorator2  
@decorator1
def my_function():
    pass

# Parameterized decorator:
def decorator_with_args(param):
    def actual_decorator(func):
        def wrapper(*args, **kwargs):
            # Use param here
            return func(*args, **kwargs)
        return wrapper
    return actual_decorator
    """)