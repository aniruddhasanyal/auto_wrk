import pandas as pd
from imblearn.combine import SMOTETomek
import logging

logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)

data = pd.read_csv('data/final_data_transpose_1016.csv')
X = data.drop(['REFERRED_FLAG', 'CLAIMID_x'], axis=1)
y = data['REFERRED_FLAG']

cols = list(X.columns.values)
cols.append('REFERRED_FLAG')

sm = SMOTETomek()

X_rs, y_rs = sm.fit_sample(X, y)

resampled = pd.concat([pd.DataFrame(X_rs), pd.Series(y_rs)], axis=1)
resampled.columns = cols
resampled.to_csv('resampled_final_train_1016.csv')

print(type(X_rs))
print(X_rs.shape)
print(sum(y_rs))

