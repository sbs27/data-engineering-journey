# python/real_api_practice.py
import requests
import pandas as pd
import time
from typing import List, Dict

class PublicDataCollector:
    """Practice with free public APIs"""
    
    def fetch_weather_data(self, city: str) -> Dict:
        """Get current weather from a free API"""
        try:
            # Using a free weather API (no key required)
            url = f"http://api.openweathermap.org/data/2.5/weather?q={city}&appid=YOUR_API_KEY&units=metric"
            
            # For practice, we'll use mock data since we don't have a real API key
            # In real scenario, you'd use the actual API call above
            
            mock_data = {
                'city': city,
                'temperature': 20 + (hash(city) % 15),  # Mock temp based on city name
                'humidity': 30 + (hash(city) % 50),
                'conditions': ['clear', 'cloudy', 'rainy'][hash(city) % 3],
                'timestamp': pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')
            }
            
            print(f" Fetched weather for {city}: {mock_data['temperature']}°C")
            return mock_data
            
        except Exception as e:
            print(f" Failed to fetch weather for {city}: {e}")
            return None
    
    def collect_multiple_cities(self, cities: List[str]) -> pd.DataFrame:
        """Collect weather data for multiple cities with error handling"""
        all_data = []
        
        for city in cities:
            print(f" Fetching data for {city}...")
            
            city_data = self.fetch_weather_data(city)
            if city_data:
                all_data.append(city_data)
            
            # Be nice - add delay between API calls
            time.sleep(1)
        
        return pd.DataFrame(all_data)
    
    def simulate_pagination(self, total_items: int = 100, page_size: int = 20):
        """Simulate paginated API responses"""
        print(f"\n SIMULATING PAGINATED API ({total_items} items, {page_size} per page)")
        
        all_items = []
        page = 1
        
        while len(all_items) < total_items:
            print(f"   Processing page {page}...")
            
            # Simulate API response
            start_idx = (page - 1) * page_size
            end_idx = min(start_idx + page_size, total_items)
            
            page_data = [
                {'id': i, 'data': f'Item_{i}', 'page': page}
                for i in range(start_idx, end_idx)
            ]
            
            all_items.extend(page_data)
            print(f"   Retrieved {len(page_data)} items (Total: {len(all_items)})")
            
            # Check if we're done
            if len(page_data) < page_size:
                break
                
            page += 1
            time.sleep(0.5)  # Simulate API rate limiting
        
        return pd.DataFrame(all_items)

def practice_error_handling():
    """Practice different error scenarios"""
    print("\n PRACTICING ERROR HANDLING SCENARIOS")
    print("="*50)
    
    collector = PublicDataCollector()
    
    # Scenario 1: Successful collection
    print("\n1. SUCCESSFUL DATA COLLECTION:")
    cities = ['London', 'Paris', 'Tokyo', 'New York']
    weather_df = collector.collect_multiple_cities(cities)
    print(weather_df)
    
    # Scenario 2: Pagination practice
    print("\n2. PAGINATION PRACTICE:")
    paginated_data = collector.simulate_pagination(total_items=75, page_size=15)
    print(f"Final result: {len(paginated_data)} total items")
    
    # Scenario 3: Data validation
    print("\n3. DATA VALIDATION:")
    if not weather_df.empty:
        print(f"   Cities processed: {len(weather_df)}")
        print(f"   Temperature range: {weather_df['temperature'].min()}°C to {weather_df['temperature'].max()}°C")
        print(f"   Unique conditions: {weather_df['conditions'].nunique()}")
    
    # Save results
    weather_df.to_parquet('data/weather_data.parquet', index=False)
    print("\n Saved weather data to Parquet format")

if __name__ == "__main__":
    practice_error_handling()