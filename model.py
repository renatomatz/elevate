import pandas as pd
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import Dense, Dropout

import clean

customers, transactions = clean.get_data("Datasource")

charities = clean.get_charities(transactions)

data = clean.clean_data(customers, transactions, charities)

cut = round(data.shape[0]*0.7)

train = data.iloc[:cut, :]
test = data.iloc[cut:, :]

X_train, y_train = clean.sep_Xy(train, charities)
X_test, y_test = clean.sep_Xy(test, charities)

model = Sequential()
model.add(Dense(250, activation="relu", input_dim=X_train.shape[1]))
model.add(Dense(150, activation="relu"))
model.add(Dense(y_train.shape[1], activation="relu"))

model.compile(optimizer="adam",
              loss="mean_squared_error",
              metrics=["accuracy", "mae", "mse"])

model.fit(X_train, y_train, batch_size=4, epochs=5)

model.evaluate(X_test, y_test)
