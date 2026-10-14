# import your other libraries here
import pandas as pd

# hint
# from sklearn.model_selection import train_test_split
# from sklearn.ensemble import ...
# from sklearn.metrics import ...
# from sklearn.impute import ...


class MushroomClassifier:
    def __init__(self, data_path):  # DO NOT modify this line
        self.data_path = data_path
        self.df = pd.read_csv(data_path)
        self.X_train = list()
        self.X_test = list()
        self.y_train = list()
        self.y_test = list()
        self.rf = None  # Your Random Forest Classfier

    def Q1(self):  # DO NOT modify this line
        """
        1. (From step 1) Before doing the data prep., how many "na" are there in "gill-size" variables?
        """
        # remove pass and replace with you code
        pass

    def Q2(self):  # DO NOT modify this line
        """
        2. (From step 2-4) How many rows of data, how many variables?
        - Drop rows where the target (label) variable is missing.
        - Drop the following variables:
        'id','gill-attachment', 'gill-spacing', 'gill-size','gill-color-rate','stalk-root', 'stalk-surface-above-ring',
        'stalk-surface-below-ring', 'stalk-color-above-ring-rate','stalk-color-below-ring-rate','veil-color-rate','veil-type'
        - Examine the number of rows, the number of digits, and whether any are missing.
        """
        # remove pass and replace with you code
        pass

    def Q3(self):  # DO NOT modify this line
        """
        3. (From step 5-6) Answer the quantity class0:class1
        - Fill missing values by adding the mean for numeric variables and the mode for nominal variables.
        - Convert the label variable e (edible) to 1 and p (poisonous) to 0 and check the quantity. class0: class1
        - Note: You need to reproduce the process (code) from Q2 to obtain the correct result.
        """
        self.Q2()  # remove this if you want to skip dependency and duplicate code
        # remove pass and replace with you code
        pass

    def Q4(self):  # DO NOT modify this line
        """
        4. (From step 7-8) How much is each training and testing sets
        - Convert the nominal variable to numeric using a dummy code with drop_first = True.
        - Split train/test with 20% test, stratify, and seed = 2020.
        - Note: You need to reproduce the process (code) from Q2, Q3 to obtain the correct result.
        """
        self.Q3()  # remove this if you want to skip dependency and duplicate code
        # remove pass and replace with you code
        pass

    def Q5(self):
        """
        5. (From step 9) 2nd best feature name based on the importance.
        Create a Random Forest on training data using following parameters:
        - 'criterion': 'gini'
        - 'max_depth': 3
        - 'min_samples_leaf': 2
        - 'N_estimators': 100
        - 'random_state': 2020
        - Note: You need to reproduce the process (code) from Q2, Q3, Q4 to obtain the correct result.
        """
        self.Q4()  # remove this if you want to skip dependency and duplicate code
        # remove pass and replace with you code

        pass

    def Q6(self):
        """
        6. (From step 10) What is the value of macro f1 (2 digits)?
        Predict the testing data set with confusion_matrix and classification_report,
        using scientific rounding (less than 0.5 dropped, more than 0.5 then increased)
        - Note: You need to reproduce the process (code) from Q2, Q3, Q4, Q5 to obtain the correct result.
        """
        self.Q5()  # remove this if you want to skip dependency and duplicate code
        # remove pass and replace with you code
        pass
