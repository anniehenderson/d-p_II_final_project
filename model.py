import statsmodels.api as sm
import pandas as pd
import os
import matplotlib.pyplot as plt
import numpy as np

base_path = r'/Users/anniehenderson/Desktop/Harris 2022 Q1 Fall/Data & Programming II/Final Project/'
model_df_file = r'model_df.csv'

model_df = pd.read_csv(os.path.join(base_path, model_df_file))

indep = model_df['crime_rate'].to_list()
dep = model_df['dom_ter_incidents'].to_list()

indep_cons = sm.add_constant(indep)

model = sm.OLS(dep, indep_cons).fit()

print(model.summary())


plt.scatter(indep, dep)

maxx = model_df['crime_rate'].max()
minx = model_df['crime_rate'].min()
 
x = np.arange(minx, maxx, 1)
y = -0.0001 * x + 2.4878

plt.plot(y, 'r')
 
scatter_file = r'model_scatter.png'
plt.savefig(os.path.join(base_path, scatter_file))
    #https://stackoverflow.com/questions/24195432/fixed-effect-in-pandas-or-statsmodels/44836199#44836199