import pandas as pd
import numpy as np

print('')
print("Hello World")
print('')

df = pd.DataFrame(np.random.randn(10, 4), columns=['a', 'b', 'c', 'd'])
print(df)

df2 = pd.DataFrame(np.random.randn(10, 4), columns=['a', 'b', 'c', 'd'])
print(df2)

print(df.equals(df2))
