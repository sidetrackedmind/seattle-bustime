# Predicting Bus Time Arrivals Across Seattle 
Check out [bus.wtf](http://bus.wtf) - my bus wait time forecast website! The goal of the project was to provide Seattle bus riders with a bus arrival prediction tool. King County provides a real-time bus location data feed for all their buses via the One Bus Away API. Real-time data works great when you’re hoping to grab a bus in the moment but it’s not helpful for future planning. In addition to bus arrival predictions, this project's aim was to provide users with insights into the route they use, confirming trends they have experienced, and provide planning insights into routes they are unfamiliar with, giving them insights that only a well-seasoned rider would know.

## Data Collection Process
In order to perform route metrics and predictions, I had to join the King County Metro's General Transit Feed Specification tables. Below is a figure showing the various table joins.
![schedule gtfs table join](/images/schedule_gtfs_tables-01-01.png)
#### Figure 1 - Diagram showing gtfs table column joins

## Modeling Process
In order to limit the scope of the project, I focused on One Bus Away's stop updates. A stop update is One Bus Away's bus arrival prediction for each stop. I took the last stop update for a given bus's trip as the "actual arrival time". This arrival time became my model's target for training. 
![modeling_diagram](/images/modeling_diagram.png)
#### Figure 2 - Diagram showing current model features and target

## Tech Stack
The following figure shows the data process flow for the project. The API updates were stored in an AWS S3 bucket. From the S3 bucket, the data is decrypted, cleaned and put into an AWS relational database. From the database, I built two model pipelines (cross validation and model fitting). The pickled model for each route was stored in my S3 bucket. When the user selects a given route and direction on the website, the appropriate model pickle is unpickled and used to predict a time given the user's inputs. 
![tech_stack](/images/tech_stack.png)
#### Figure 3 - Diagram showing tech stack used

## Modeling Testing
I quality checked the model predictions by plotting one bus's trip across a route. The figure below shows a route 45 bus's trip. The gray is the scheduled time, the blue is the actual trip, and the red is my prediction. For this route, the model does a good job fitting the general shape of the route. 
![Model testing](/images/bus_45_trip_preds.png)
#### Figure 4 - Testing predictions vs. actual and scheduled times for one 45 bus at 8:44AM on Jan 23 2018

Since the model trained on historical data, it cannot adapt to unexpected traffic flucuations. The graph below shows an unexpected slowdown near the University of Washington. The model prediciton follows the general shape but the actual time has been shifted up as a result of traffic. If this traffic happens consistently, the model would start to pick up on it and change shape. If the traffic blocks are irregular, this particular model will not improve.
![Model testing](/images/bus_45_trip_preds_2.png)
#### Figure 5 - Testing predictions vs. actual and scheduled times for one 45 bus at 7:43AM on Jan 23 2018

