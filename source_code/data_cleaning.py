import pandas as pd
import pandasql as ps

sentiment1 =  pd.read_csv('AllFoodBusiness_Features&Sentiments.csv')
sentiment = ps.sqldf("""select 
                        business_id,
                        round(sentimental_rating,0) as sentimental_rating
                        from sentiment1""", locals())
sentiment.describe()
sentiment.head()

business = pd.read_csv('business.csv')
business.rename(columns = {'stars':'business_stars'}, inplace = True)
business.head()
business.describe()
business.columns

####### REVIEW
review = pd.read_csv('review.csv', iterator=True, chunksize=500)
review = pd.concat(review, ignore_index =True)
review.describe()
review.columns
review.head()
business.head()

business_eateries = business[(business['categories'].str.contains(pat = 'Restaurants',na=False)) |
                                       (business['categories'].str.contains(pat = 'Lounges',na=False)) |
                                        (business['categories'].str.contains(pat = 'Nightlife',na=False)) |
                                        (business['categories'].str.contains(pat = 'Bars',na=False)) |
                                        (business['categories'].str.contains(pat = 'Food',na=False)) |
                                        (business['categories'].str.contains(pat = 'Coffee&Tea',na=False))|
                                        (business['categories'].str.contains(pat = 'Bakeries',na=False))|
                                        (business['categories'].str.contains(pat = 'Pubs',na=False))|
                                        (business['categories'].str.contains(pat = 'Pizza',na=False))]
business_eateries.describe()

business_eateries.columns
business_eateries.head()

cols = business_eateries.columns
cols = cols.map(lambda x: x.replace('.', '_'))
business_eateries.columns = cols
# write business_eateries csv
business_eateries.to_csv('Businesses_Eateries.csv')
business_eateries.head()

business_eateries_sentiment = pd.merge(business_eateries, sentiment, on = 'business_id')
business_eateries_sentiment.to_csv('Business_Eateries_Sentiment.csv')

business_review = pd.merge(business, review, on = 'business_id')
review_grouped = business_review.groupby(['city' , 'business_id'], as_index=False).mean()
data = review_grouped[['business_id','stars']]
review_eateries = data.apply(lambda x: x)
review_eateries.rename(columns={'stars': 'review_avg_stars'}, inplace=True)
review_eateries.head()

business_reviews_eateries = pd.merge(business_eateries, review_eateries, on = 'business_id')
business_reviews_eateries.head()
print(business_reviews_eateries.columns)

business_relevant_review_eateries = business_reviews_eateries[['business_id','latitude', 'longitude','review_count'
                  ,'business_stars','review_avg_stars','attributes_RestaurantsPriceRange2','attributes_BusinessAcceptsCreditCards','attributes_RestaurantsTakeOut','attributes_RestaurantsDelivery',
                        'attributes_WheelchairAccessible','attributes_GoodForMeal_breakfast','attributes_GoodForMeal_latenight','attributes_GoodForMeal_dessert','attributes_GoodForMeal_lunch',
                        'attributes_GoodForMeal_brunch','attributes_RestaurantsReservations','attributes_BusinessParking_validated','attributes_BusinessParking_valet','attributes_BusinessParking_lot','attributes_BusinessParking_garage',
                        'attributes_BusinessParking_street','attributes_BikeParking','state','city','name','attributes_GoodForKids','attributes_RestaurantsGoodForGroups','attributes_Ambience_trendy','attributes_Ambience_casual','attributes_Ambience_classy','attributes_Ambience_touristy','attributes_Ambience_intimate'
                        ,'attributes_Ambience_hipster']]


business_relevant_review_eateries.head()

business_relevant_review_eateries_sentiment = pd.merge(business_relevant_review_eateries, sentiment, on = 'business_id')
business_relevant_review_eateries_sentiment.to_csv('Model_Input.csv')
business_relevant_review_eateries_sentiment.head()


