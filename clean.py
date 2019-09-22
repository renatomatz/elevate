import pandas as pd

def get_data(f):

    return (pd.read_csv(f+"/output_customer.csv"), 
            pd.read_csv(f+"/output_transaction.csv"))


def get_charities(t):
    mask = t.categoryTags.map(lambda x: x == "Food and Dining")
    return pd.unique(t.loc[mask, "merchantId"])


def clean_data(cust, tr, charities):
 
    cust = cust[["id", "age", "gender", "totalIncome", "occupationIndustry", 
                 "relationshipStatus", "habitationStatus", "postalCode", 
                 "schoolAttendance"]]

    tr = tr[["custId", "currencyAmount", "merchantId"]]
    tr.currencyAmount = tr.currencyAmount.map(abs)

    dons = tr.groupby(["custId", "merchantId"]).sum().reset_index(["custId", "merchantId"])

    dons = dons.merge(cust.loc[:, ["id", "totalIncome"]], how="left", left_on="custId", right_on="id")

    dons.currencyAmount = dons.currencyAmount / dons.totalIncome
    dons = dons.drop(["id", "totalIncome"], axis=1)

    dons = pd.pivot_table(dons, values="currencyAmount", index="custId", columns="merchantId", fill_value=0) \
             .reset_index("custId")

    data = dons.merge(cust, how="left", left_on="custId", right_on="id") \
               .drop("id", axis=1) \
               .fillna(0)

    data = pd.get_dummies(data, columns=["gender",  
                                         "occupationIndustry", 
                                         "relationshipStatus", 
                                         "habitationStatus", 
                                         "postalCode", "schoolAttendance"])


    has_donated = data.loc[:, charities].fillna(0).apply(lambda row: sum(row) > 0, axis=1)

    data = data.loc[has_donated, :].fillna(0)

    return data




def sep_Xy(df, charities):

    mask = df.columns.map(lambda x: x in charities)
    n_mask = mask.map(lambda x: not x)
    return df.iloc[:, n_mask].drop("custId", axis=1), df.iloc[:, mask]


# if __name__ == "__main__":

#   customers, transactions = get_data("Datasource")

#    charities = get_charities(transactions)

#    data = clean_data(customers, transactions, charities)

#    X, y = sep_Xy(data, charities)

