Inorder to run the program from end to end follow the steps :

Step 1 - Download the yelp data set from https://www.yelp.com/dataset/challenge. Download the json format dataset.
Step 2 - extract the zip folder and get two json files : business.json and review.json
Step 3 - copy the two files in  project folder and run data_preprocessing.py file which will convert both the files in csv format.
step 4 - sentiment.csv file is already provided which was formed by doing sentiment analysis using map reduce on AWS EC2 instances . 
It contains sentimental ratings of businesses. You can go through the map reduce code in mapreduce.py file.
step 5 - run the data_cleaning.py file which will select the relevant features from business.csv and merge sentiment.csv and business.csv file.
         A new file Model_Input.csv will be obtained.

Step 6 - Use Model_Input.csv as an input to classifiers. There are two examples SVM_model and randomforest_model. You can run them to visualize results.

Step 7 - Run baseline.py file to see accuracy of SVM classifier for certain locations.

To see all the steps from end to end open IDS_Final_Project_Visualization.ipynb on jupyter notebook and run each cell one by one after downloading the dataset.