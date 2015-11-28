import seldon.pipeline.sklearn_transform as ssk
import pandas as pd
from sklearn.preprocessing import StandardScaler

df = pd.DataFrame.from_dict([{"a":1.0,"b":2.0},{"a":2.0,"b":3.0}])
t = ssk.sklearn_transform(input_features=["a"],output_features=["a_scaled"],transformer=StandardScaler())
t.fit(df)
df_2 = t.transform(df)
print df_2
