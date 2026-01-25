import os
filepath = 'state_crawled_stocks.json'
current_dir = os.path.dirname(os.path.abspath(__file__))
full_path = os.path.join(current_dir, filepath)
print(full_path)