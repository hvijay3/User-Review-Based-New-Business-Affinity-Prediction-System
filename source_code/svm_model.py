from collections import defaultdict
import numpy as np
from sklearn.svm import SVC
from sklearn.cross_validation import train_test_split
from sklearn.utils import column_or_1d
from sklearn import preprocessing
from sklearn.ensemble import RandomForestClassifier
import csv
import pandas as pd
import pandasql as ps
import sys
import matplotlib.pyplot as plt

from sklearn.model_selection import validation_curve

reload(sys)
sys.setdefaultencoding("utf-8")
%matplotlib inline

attributes = ['attributes_RestaurantsPriceRange2',
              'attributes_BusinessAcceptsCreditCards',
              'attributes_RestaurantsTakeOut',
              'attributes_RestaurantsDelivery',
              'attributes_WheelchairAccessible',
              'attributes_GoodForMeal_breakfast',
              'attributes_GoodForMeal_latenight',
              'attributes_GoodForMeal_dessert',
              'attributes_GoodForMeal_lunch',
              'attributes_GoodForMeal_brunch',
              'attributes_RestaurantsReservations',
              'attributes_BusinessParking_validated',
              'attributes_BusinessParking_valet',
              'attributes_BusinessParking_lot',
              'attributes_BusinessParking_garage', 
              'attributes_BusinessParking_street',
              'attributes_BikeParking',
              'attributes_GoodForKids',
              'attributes_RestaurantsGoodForGroups',
              'attributes_Ambience_trendy',
              'attributes_Ambience_casual',
              'attributes_Ambience_classy',
              'attributes_Ambience_touristy',
              'attributes_Ambience_intimate',
              'attributes_Ambience_hipster',
              'latitude',
              'longitude',
              'business_stars', 
              'review_count', 
              'sentimental_rating',
              'review_avg_stars']

print('---- Reading Model_input.csv -------- ')

model_input_dict = defaultdict(dict)           

with open('Model_Input.csv') as csvfile:
    
    reader = csv.DictReader(csvfile)
    
    for row in reader:
        for attribute in attributes[:25]:
            if row[attribute] == 'TRUE':
                row[attribute] = 1
            elif row[attribute] == 'FALSE':
                row[attribute] = 0
            else:
                row[attribute] = 0
        
        model_input_dict[row['city']].update({ 
                                            row['business_id'] : 
                                            [ 
                                                int(row[attributes[0]]),
                                                int(row[attributes[1]]), 
                                                int(row[attributes[2]]), 
                                                int(row[attributes[3]]), 
                                                int(row[attributes[4]]), 
                                                int(row[attributes[5]]), 
                                                int(row[attributes[6]]), 
                                                int(row[attributes[7]]), 
                                                int(row[attributes[8]]), 
                                                int(row[attributes[9]]), 
                                                int(row[attributes[10]]), 
                                                int(row[attributes[11]]),
                                                int(row[attributes[12]]),
                                                int(row[attributes[13]]),
                                                int(row[attributes[14]]),
                                                int(row[attributes[15]]),
                                                int(row[attributes[16]]),
                                                int(row[attributes[17]]),
                                                int(row[attributes[18]]),
                                                int(row[attributes[19]]),
                                                int(row[attributes[20]]),
                                                int(row[attributes[21]]),
                                                int(row[attributes[22]]),
                                                int(row[attributes[23]]),
                                                int(row[attributes[24]]),
                                                float(row[attributes[25]]),
                                                float(row[attributes[26]]),
                                                float(row[attributes[27]]),
                                                int(row[attributes[28]]),
                                                float(row[attributes[29]]),
                                                int(numpy.round(float(row[attributes[30]]))),
                                            ]
                                            })


print('------Model Making Started-------')

results = defaultdict(list)
cities_models = defaultdict()

for city, business_dict in model_input_dict.items():
    if len(business_dict)<20 :
        continue
    final_features = []
    final_labels = []
    #print(city)
    #print(business_dict)
    for business, features_list in business_dict.items():
        #print(business)
        #print(features_list)
        final_features.append(features_list[:-1]) #everything except the last item 
        final_labels.append(features_list[-1]) #last label
        

        
   
    
    #print(final_features)
    #print(final_labels)
    features_train, features_test, labels_train, labels_test = train_test_split(numpy.array(final_features), numpy.array(final_labels), 
                                                       test_size=0.2, random_state = 42)
    #print(features_train)
    #print(features_test)
    #print(labels_train)
    #print(labels_test)
                                                                                
    #labels_train = column_or_1d(labels_train, warn=False)
    #labels_test = column_or_1d(labels_test, warn=False)          
    #print(labels_train)
    #print(labels_test)
    features_train_transformed = preprocessing.StandardScaler().fit_transform(features_train)
    features_test_transformed = preprocessing.StandardScaler().fit_transform(features_test)
    
    clf = SVC(C=1).fit(features_train_transformed, labels_train)
    train_accuracy = clf.score(features_train_transformed, labels_train)
    test_accuracy = clf.score(features_test_transformed, labels_test)
    #print(train_accuracy)
    #print(test_accuracy)
    

    if train_accuracy >=0.65 and test_accuracy>=0.60:
        cities_models[city] = clf
        results[city].append(features_train.shape)
        results[city].append(features_test.shape)
        results[city].append(train_accuracy)
        results[city].append(test_accuracy)
        
print('------Write accuracy in file-------')
final_results = []
for u, r in results.items():
    train_rows = r[0][0]
    test_rows = r[1][0]
    final_results.append([u, train_rows, test_rows, r[2], r[3]])
    
resultFile = open('Cities_Model_Fit_Accuracies_SVM.csv','w')
wr = csv.writer(resultFile, delimiter=',', lineterminator='\n' )
wr.writerow(['city', 'features_train_rows' , 'features_test_rows', 'train_accuracy', 'test_accuracy'])
wr.writerows(final_results)
resultFile.close()

# visualize train and test accuracies
sentiment = pd.read_csv('Cities_Model_Fit_Accuracies_SVM.csv')
sentiment = sentiment.head(5)
sentiment['train_accuracy'] = sentiment['train_accuracy'].astype(float)
sentiment['test_accuracy'] = sentiment['test_accuracy'].astype(float)
fig,ax1 = plt.subplots()
ax2 = ax1.twinx()
x = sentiment.city
xn = range(len(x))
plt.xticks(xn, sentiment['city'])
ax1.plot(xn, sentiment['train_accuracy'], 'g-')
ax2.plot(xn, sentiment['test_accuracy'], 'r-')
ax1.set_xlabel('City')
ax1.set_ylabel('Train Accuracy', color='g')
ax2.set_ylabel('Test Accuracy', color='r')