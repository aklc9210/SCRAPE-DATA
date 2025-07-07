import pandas as pd

# Đọc file CSV (thay 'your_file.csv' bằng đường dẫn tới file của bạn)
df = pd.read_csv('categories.csv')

# Đếm số giá trị unique trong cột 'category'
n_unique = df['name'].nunique()

print(f"Số giá trị unique trong cột 'category': {n_unique}")