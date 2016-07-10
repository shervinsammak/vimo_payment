Insight Data Challenge Code Submission

Hi I am Shervin Sammak graduate student in Computational modeling and simulation at the University of Pittsburgh.

The challenge is about to use venmo payments to build a graph of users and calculate the median degree of a vertex in a graph. I have implemented the project in Python 3.5.1. Before executing the project, kindly check i you have the following installed in your system.

1. json
2. sys
3. datetime
4. dateutil.parser

If any of the library is not presented please install them using 'pip'.

eg: pip install datetime

Feature extraction:
-----------------------
1. I have processed the venmo payment from the venmo-trans.txt, and extracted the actor, target and timestep using 'actor', 'target' and 'created_time' tags.
2. Collected the 'actor' and 'target' from every payment which is in between 60 sec and created a payment graph. 
3. Calculated the average degrees and output could be found in venmo_output/.
