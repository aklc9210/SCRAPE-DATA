import pandas as pd

# Đọc file CSV (thay 'your_file.csv' bằng đường dẫn tới file của bạn)
df = pd.read_csv('categories.csv')

# Đếm số giá trị unique trong cột 'category'
n_unique = df['name'].nunique()

print(f"Số giá trị unique trong cột 'category': {n_unique}")

df1 = pd.read_csv('provinces.csv')

print(f"Số tỉnh thành: {df1['name'].unique()}, {df1['name'].nunique()}, {df1['id'].max()}, {df1['id'].min()}")

print ("Store")