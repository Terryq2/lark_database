from src.driver import DataSyncClient

if __name__ == "__main__":
    test = DataSyncClient(".env", "config.json")
    test.sync_most_recent_data('C03', 'TETST')
    
    

