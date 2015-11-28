import seldon.pipeline.auto_transforms as auto
import pandas as pd

df = pd.DataFrame([{"a":10,"b":1,"c":"cat"},{"a":5,"b":2,"c":"dog","d":"Nov 13 08:36:29 2015"},{"a":10,"b":3,"d":"Oct 13 10:50:12 2015"}])
t = auto.Auto_transform(max_values_numeric_categorical=2,date_cols=["d"])
t.fit(df)
df2 = t.transform(df)
print df2
