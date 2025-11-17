import time
import sys
import os

def create_large_log_file():
    """Create a massive log file for testing memory efficiency"""
    print("📁 Creating large log file (100,000 lines)...")
    
    with open('data/large_server_logs.txt', 'w') as f:
        for i in range(100000):
            # Simulate different log types
            log_types = ['INFO', 'WARNING', 'ERROR', 'DEBUG']
            log_type = log_types[i % 4]
            
            # Simulate different user actions
            actions = ['login', 'purchase', 'view_page', 'logout', 'search']
            action = actions[i % 5]
            
            # Write formatted log line
            f.write(f"2025-11-17 10:{i%60:02d}:{i%60:02d} {log_type} User_{i%1000} {action} product_{i%50} value={i}\n")
    
    file_size = os.path.getsize('data/large_server_logs.txt')
    print(f" Created data/large_server_logs.txt ({file_size:,} bytes)")

def process_with_generator(file_path):
    """
    Memory-efficient generator approach
    Processes one line at a time without loading entire file into RAM
    """
    print("\n🔄 USING GENERATOR APPROACH (Memory Efficient)")
    print("   Processing one line at a time...")
     # Initialize counters to track our progress
    processed_count = 0
    error_count = 0
    
    with open(file_path, 'r') as file:
        for line_number, line in enumerate(file, 1):
            try:
                # Process each line individually
                processed_line = line.strip().upper()
                
                # Simulate some data processing
                if 'ERROR' in processed_line:
                    error_count += 1
                
                # Yield the processed result
                 # This is what makes it a generator function
                yield line_number, processed_line, error_count
                
                processed_count = line_number
                
                # Show progress every 10,000 lines
                if line_number % 10000 == 0:
                    print(f"  Processed {line_number:,} lines... (Errors: {error_count})")
                    
            except Exception as e:
                print(f"  Error processing line {line_number}: {e}")
                continue
    
    print(f" Generator completed: {processed_count:,} total lines processed")

def process_with_list(file_path):
    """
    Memory-intensive list approach
    Loads entire file into RAM - dangerous with large files!
    """
    print("\n USING LIST APPROACH (Memory Intensive)")
    print("   Loading entire file into memory...")
    # Initialize empty list to store ALL processed data
    all_processed_data = []
    error_count = 0
    processed_count = 0
    
    try:
        with open(file_path, 'r') as file:
            # DANGER: Reading entire file into memory!
            all_lines = file.readlines()
            
            print(f"  Loaded {len(all_lines):,} lines into RAM")
            
            for line_number, line in enumerate(all_lines, 1):
                # Process each line
                processed_line = line.strip().upper()
                
                if 'ERROR' in processed_line:
                    error_count += 1
                
                # Store everything in memory
                all_processed_data.append((line_number, processed_line, error_count))
                processed_count = line_number
                
                if line_number % 10000 == 0:
                    print(f"  Stored {line_number:,} lines in memory...")
        
        print(f"  List approach completed: {len(all_processed_data):,} lines stored in RAM")
        return all_processed_data
        
    except MemoryError:
        print(" MEMORY ERROR! File too large to load into RAM")
        return []

def demonstrate_memory_efficiency():
    """Compare generator vs list approaches"""
    print("=" * 60)
    print(" MEMORY EFFICIENCY DEMONSTRATION: GENERATOR vs LIST")
    print("=" * 60)
    
    # STEP 1: Create our test data (100,000 line log file)
    create_large_log_file()
    
    # STEP 2: Test Generator Approach
    print("\n1. GENERATOR APPROACH (Lazy Evaluation):")
    print("   - Processes one item at a time")
    print("   - Minimal memory usage")
    print("   - Can handle files larger than RAM")
    print("   - Results:")
    # process_with_generator() returns a GENERATOR OBJECT
    # Not the data itself! It's ready to produce data when asked
    start_time = time.time()
    generator = process_with_generator('data/large_server_logs.txt')
    
    # Process just enough to demonstrate it works
    sample_count = 0
    for line_num, content, errors in generator:
        if sample_count < 3:  # Show first 3 samples
            print(f"      Sample {sample_count + 1}: {content[:80]}...")
        sample_count += 1
        if sample_count >= 1000:  # Stop early for demo
            break
    
    gen_time = time.time() - start_time
    print(f" Processed {sample_count:,} lines in {gen_time:.2f}s")
    
    # STEP 3: Test List Approach
    print("\n2. LIST APPROACH (Eager Evaluation):")
    print("   - Loads everything into memory at once")
    print("   - High memory usage")
    print("   - Crashes with files larger than RAM")
    print("   - Results:")
    
    start_time = time.time()
    all_data = process_with_list('data/large_server_logs.txt')
    
    if all_data:
        for i, (line_num, content, errors) in enumerate(all_data[:3]):
            print(f"      Sample {i + 1}: {content[:80]}...")
        
        list_time = time.time() - start_time
        print(f" Processed {len(all_data):,} lines in {list_time:.2f}s")
    
    # Key Insights
    print("\n" + "=" * 60)
    print("💡 KEY INSIGHTS FOR DATA ENGINEERING:")
    print("   • Generators: Use for large datasets, streaming data, memory constraints")
    print("   • Lists: Use for small datasets that fit comfortably in memory")
    print("   • Real-world: Data engineers often process GB/TB of data - generators are essential!")
    print("=" * 60)

if __name__ == "__main__":
    demonstrate_memory_efficiency()