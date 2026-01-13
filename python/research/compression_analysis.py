
def test_compression_types():
    """Test different Parquet compression algorithms"""
    df = create_realistic_dataset()
    
    compressions = ['snappy', 'gzip', 'brotli', 'none']
    
    print("\n COMPRESSION ALGORITHM COMPARISON")
    print("="*50)
    
    for compression in compressions:
        filename = f'data/dataset_{compression}.parquet'
        
        start = time.time()
        df.to_parquet(filename, compression=compression, index=False)
        write_time = time.time() - start
        
        start = time.time()
        pd.read_parquet(filename)
        read_time = time.time() - start
        
        size = os.path.getsize(filename)
        
        print(f"\n{compression.upper():<8}: {write_time:5.2f}s write, {read_time:5.2f}s read, {size:>10,} bytes")

